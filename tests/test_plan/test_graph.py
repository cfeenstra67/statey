import hashlib
import os

import pytest
import statey as st
from statey.resource.lib.os import File


@pytest.mark.asyncio
async def test_build_graph(state, graph, tmpdir):

    file1 = File["file1"](path=os.path.join(tmpdir, "file1.txt"), data="This is file 1.")
    graph.add(file1)
    file2 = File["file2"](
        path=os.path.join(tmpdir, "file2.txt"),
        data=st.f("This was file 1's hash: {file1.attrs.data_sha256}"),
    )
    graph.add(file2)

    plan = await state.plan(graph)
    assert len(plan.graph) == 2
    assert plan.graph.nodes["file:/file1"]["change"].resource is file1
    assert type(plan.graph.nodes["file:/file1"]["change"]).__name__ == "Create"
    file1_content = "This is file 1."
    assert plan.graph.nodes["file:/file1"]["change"].snapshot.copy(
        size_bytes=None
    ) == file1.schema_helper.snapshot_cls(
        path=os.path.join(tmpdir, "file1.txt"),
        data=file1_content,
        data_sha256=hashlib.sha256(file1_content.encode()).hexdigest(),
        size_bytes=None,
        permissions=420,
    )
    assert plan.graph.nodes["file:/file2"]["change"].resource is file2
    assert type(plan.graph.nodes["file:/file2"]["change"]).__name__ == "Create"
    file2_content = f'This was file 1\'s hash: {hashlib.sha256(b"This is file 1.").hexdigest()}'
    assert plan.graph.nodes["file:/file2"]["change"].snapshot.copy(
        size_bytes=None
    ) == file2.schema_helper.snapshot_cls(
        path=os.path.join(tmpdir, "file2.txt"),
        data=file2_content,
        data_sha256=hashlib.sha256(file2_content.encode()).hexdigest(),
        size_bytes=None,
        permissions=420,
    )

    pretty_plan = plan.pretty_print()
    assert len(pretty_plan) == 2
    assert pretty_plan[0]["path"] == "file:/file1"
    assert pretty_plan[1]["path"] == "file:/file2"
    assert pretty_plan[1]["after"] == ["file:/file1"]


@pytest.mark.asyncio
async def test_build_graph_apply(state, graph, tmpdir):

    file1 = File["file1"](path=os.path.join(tmpdir, "file1.txt"), data="This is file 1.")
    graph.add(file1)
    file2 = File["file2"](
        path=os.path.join(tmpdir, "file2.txt"),
        data=st.f("This was file 1's hash: {file1.attrs.data_sha256}"),
    )
    graph.add(file2)

    plan = await state.plan(graph)
    result = await state.apply(plan)

    assert result.success
    assert len(result.complete) == 2

    with open(os.path.join(tmpdir, "file1.txt")) as f:
        assert f.read() == "This is file 1."

    with open(os.path.join(tmpdir, "file2.txt")) as f:
        assert (
            f.read() == f'This was file 1\'s hash: {hashlib.sha256(b"This is file 1.").hexdigest()}'
        )


@pytest.mark.asyncio
async def test_build_graph_no_change(state, graph, tmpdir):

    file1 = File["file1"](path=os.path.join(tmpdir, "file1.txt"), data="This is file 1.")
    graph.add(file1)
    file2 = File["file2"](
        path=os.path.join(tmpdir, "file2.txt"),
        data=st.f("This was file 1's hash: {file1.attrs.data_sha256}"),
    )
    graph.add(file2)

    plan = await state.plan(graph)
    result = await state.apply(plan)

    assert result.success

    new_plan = await state.plan(graph)
    assert len(new_plan.pretty_print()) == 0


@pytest.mark.asyncio
async def test_build_graph_change_apply(state, graph, tmpdir):

    file1 = File["file1"](path=os.path.join(tmpdir, "file1.txt"), data="This is file 1.")
    graph.add(file1)
    file2 = File["file2"](
        path=os.path.join(tmpdir, "file2.txt"),
        data=st.f("This was file 1's hash: {file1.attrs.data_sha256}"),
    )
    graph.add(file2)

    plan = await state.plan(graph)
    result = await state.apply(plan)

    assert result.success

    new_graph = state.graph()

    file1 = File["file1"](path=os.path.join(tmpdir, "file1.txt"), data="This is file 1. This too!")
    new_graph.add(file1)
    file2 = File["file2"](
        path=os.path.join(tmpdir, "file2.txt"),
        data=st.f("This was file 1's hash: {file1.attrs.data_sha256}"),
    )
    new_graph.add(file2)

    plan = await state.plan(new_graph)

    pretty_plan = plan.pretty_print()
    assert len(pretty_plan) == 2
    assert pretty_plan[0]["path"] == "file:/file1"
    assert pretty_plan[0]["type"] == "Update"
    assert pretty_plan[0]["after"] == []
    assert pretty_plan[1]["path"] == "file:/file2"
    assert pretty_plan[1]["type"] == "Update"
    assert pretty_plan[1]["after"] == ["file:/file1"]

    result = await state.apply(plan)
    assert result.success

    with open(os.path.join(tmpdir, "file1.txt")) as f:
        assert f.read() == "This is file 1. This too!"

    with open(os.path.join(tmpdir, "file2.txt")) as f:
        assert (
            f.read()
            == f'This was file 1\'s hash: {hashlib.sha256(b"This is file 1. This too!").hexdigest()}'
        )


@pytest.mark.asyncio
async def test_build_graph_teardown_apply(state, graph, tmpdir):

    file1 = File["file1"](path=os.path.join(tmpdir, "file1.txt"), data="This is file 1.")
    graph.add(file1)
    file2 = File["file2"](
        path=os.path.join(tmpdir, "file2.txt"),
        data=st.f("This was file 1's hash: {file1.attrs.data_sha256}"),
    )
    graph.add(file2)

    plan = await state.plan(graph)
    result = await state.apply(plan)

    assert result.success

    empty_graph = state.graph()
    plan = await state.plan(empty_graph)

    pretty_plan = plan.pretty_print()
    assert len(pretty_plan) == 2
    assert pretty_plan[0]["path"] == "file:/file2"
    assert pretty_plan[0]["type"] == "Delete"
    assert pretty_plan[0]["after"] == []
    assert pretty_plan[1]["path"] == "file:/file1"
    assert pretty_plan[1]["type"] == "Delete"
    assert pretty_plan[1]["after"] == ["file:/file2"]

    result = await state.apply(plan)
    assert result.success

    assert not os.path.exists(os.path.join(tmpdir, "file1.txt"))
    assert not os.path.exists(os.path.join(tmpdir, "file2.txt"))


@pytest.mark.asyncio
async def test_build_graph_remove_apply(state, graph, tmpdir):

    file1 = File["file1"](path=os.path.join(tmpdir, "file1.txt"), data="This is file 1.")
    graph.add(file1)
    file2 = File["file2"](
        path=os.path.join(tmpdir, "file2.txt"),
        data=st.f("This was file 1's hash: {file1.attrs.data_sha256}"),
    )
    graph.add(file2)

    plan = await state.plan(graph)
    result = await state.apply(plan)

    assert result.success

    new_graph = state.graph()
    new_graph.add(file1)

    plan = await state.plan(new_graph)
    pretty_plan = plan.pretty_print()
    assert len(pretty_plan) == 1
    assert pretty_plan[0]["path"] == "file:/file2"
    assert pretty_plan[0]["type"] == "Delete"
    assert pretty_plan[0]["after"] == []

    result = await state.apply(plan)
    assert result.success

    with open(os.path.join(tmpdir, "file1.txt")) as f:
        assert f.read() == "This is file 1."

    assert not os.path.exists(os.path.join(tmpdir, "file2.txt"))
