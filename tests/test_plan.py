import json
import os
import textwrap as tw
from datetime import datetime, date

import pytest

import statey as st
from tests.resources.file import File


@st.function
def today() -> str:
    return date.today().isoformat()


@st.function
def json_dumps(value: st.Any) -> str:
    return json.dumps(value, sort_keys=True)


@st.declarative
def files_test_1(session, tmpdir):
    file_1 = File(location=tmpdir + "/test-1.txt", data="Hello, world!")
    file_2 = File(
        location=file_1.location + ".backup",
        data=st.f(
            tw.dedent(
                """
                Copy of {file_1.location}:
                --------------------------
                {file_1.data}
                --------------------------
                Copied {today()}
                """
            ).strip()
        ),
    )
    file_3 = File(
        location=file_1.location + ".backup.json",
        data=json_dumps(
            {"data": file_1.data, "location": file_1.location, "copied": today()}
        ),
    )


@st.declarative
def files_test_2(session, tmpdir):
    file_1 = File(location=tmpdir + "/test-2.txt", data="Hello, world! 2")
    file_2 = File(
        location=file_1.location + ".backup",
        data=st.f(
            tw.dedent(
                """
                Copy of {file_1.location}:
                --------------------------
                {file_1.data}
                --------------------------
                Copied {today()}
                """
            ).strip()
        ),
    )


@st.declarative
def files_test_3(session, tmpdir):
    file_1 = File(location=tmpdir + "/test-3.txt", data="Hello, world! 2")
    file_2 = File(
        location=tmpdir + "/test-2.txt.backup",
        data=st.f(
            tw.dedent(
                """
                Copy of {file_1.location}:
                --------------------------
                {file_1.data}
                --------------------------
                Copied {today()}
                """
            ).strip()
        ),
    )


def assert_depends(a, b, graph):
    assert b in graph.pred[a]


async def setup_state(func, *args, **kwargs):

    session = st.create_resource_session()
    func(session, *args, **kwargs)

    migrator = st.DefaultMigrator()

    plan = await migrator.plan(session)

    executor = st.AsyncIOGraphExecutor()

    exec_info = await executor.execute_async(plan.task_graph)

    exec_info.raise_for_failure()

    rg = plan.task_graph.resource_graph

    async for _ in rg.refresh():
        pass

    return rg


@pytest.mark.asyncio
async def test_plan_create(tmpdir, resource_session, migrator, executor):

    session = resource_session
    files_test_1(session, tmpdir)

    plan = await migrator.plan(session)

    assert not plan.is_empty()

    assert len(plan.nodes) == 3

    nodes_by_key = {node.key: node for node in plan.nodes}

    keys = ["file_1", "file_2", "file_3"]

    assert set(nodes_by_key) == set(keys)

    task_graph = plan.task_graph.task_graph
    resource_graph = plan.task_graph.resource_graph

    assert set(task_graph.nodes) == {
        f"{first}:{second}"
        for first in keys
        for second in ["input", "state", "output", "task:create_file"]
    }

    for key in keys:
        assert_depends(f"{key}:task:create_file", f"{key}:input", task_graph)
        assert_depends(f"{key}:state", f"{key}:task:create_file", task_graph)
        assert_depends(f"{key}:output", f"{key}:state", task_graph)

    assert_depends("file_2:input", "file_1:output", task_graph)
    assert_depends("file_3:input", "file_1:output", task_graph)

    exec_info = await executor.execute_async(plan.task_graph)

    assert exec_info.is_success()

    tasks_by_status = exec_info.tasks_by_status()

    assert set(tasks_by_status) == {st.TaskStatus.SUCCESS}

    assert len(tasks_by_status[st.TaskStatus.SUCCESS]) == 4 * 3

    data = resource_graph.graph.nodes

    file_1_data = data["file_1"]["value"]
    file_1_content = "Hello, world!"

    assert file_1_data["location"] == os.path.realpath(tmpdir + "/test-1.txt")
    assert file_1_data["data"] == ""
    with open(file_1_data["location"]) as f:
        assert f.read() == file_1_content

    file_2_data = data["file_2"]["value"]
    file_2_content = tw.dedent(
        f"""
        Copy of {file_1_data["location"]}:
        --------------------------
        {file_1_content}
        --------------------------
        Copied {date.today().isoformat()}
        """
    ).strip()

    assert file_2_data["location"] == os.path.realpath(tmpdir + "/test-1.txt.backup")
    assert file_2_data["data"] == ""
    with open(file_2_data["location"]) as f:
        assert f.read() == file_2_content

    file_3_data = data["file_3"]["value"]
    file_3_content = json.dumps(
        {
            "location": file_1_data["location"],
            "data": file_1_content,
            "copied": date.today().isoformat(),
        },
        sort_keys=True,
    )

    assert file_3_data["location"] == os.path.realpath(
        tmpdir + "/test-1.txt.backup.json"
    )
    assert file_3_data["data"] == ""
    with open(file_3_data["location"]) as f:
        assert f.read() == file_3_content

    async for _ in resource_graph.refresh():
        pass

    # This should load the real data into the graph
    for key in keys:
        file_data = data[key]["value"]
        with open(file_data["location"]) as f:
            assert f.read() == file_data["data"]

    # Make sure planning again yields an empty plan

    plan_post_update = await migrator.plan(session, resource_graph)

    assert plan_post_update.is_empty()


@pytest.mark.asyncio
async def test_update_recreate(tmpdir, resource_session, migrator, executor):

    resource_graph = await setup_state(files_test_1, tmpdir)
    keys = ["file_1", "file_2", "file_3"]

    session_2 = resource_session
    files_test_2(session_2, tmpdir)

    plan_2 = await migrator.plan(session_2, resource_graph)

    task_graph = plan_2.task_graph.task_graph
    resource_graph = plan_2.task_graph.resource_graph

    nodes_by_key = {node.key: node for node in plan_2.nodes}

    assert set(nodes_by_key) == set(keys)

    assert set(task_graph.nodes) == {
        "file_3:input",
        "file_3:output",
        "file_3:task:delete_file",
    } | {
        f"{first}:{second}"
        for first in ["file_1", "file_2"]
        for second in [
            "current:input",
            "current:output",
            "current:task:delete_file",
            "config:input",
            "config:state",
            "config:output",
            "config:task:create_file",
        ]
    }

    assert_depends("file_1:current:input", "file_3:output", task_graph)
    assert_depends("file_1:current:input", "file_2:current:output", task_graph)
    assert_depends("file_1:config:input", "file_1:current:output", task_graph)
    assert_depends("file_2:config:input", "file_1:config:output", task_graph)
    assert_depends("file_2:config:input", "file_2:current:output", task_graph)

    exec_info = await executor.execute_async(plan_2.task_graph)

    assert exec_info.is_success()

    tasks_by_status = exec_info.tasks_by_status()

    assert set(tasks_by_status) == {st.TaskStatus.SUCCESS}

    assert len(tasks_by_status[st.TaskStatus.SUCCESS]) == len(task_graph.nodes)

    data = resource_graph.graph.nodes

    file_1_data = data["file_1"]["value"]
    file_1_content = "Hello, world! 2"

    assert file_1_data["location"] == os.path.realpath(tmpdir + "/test-2.txt")
    assert file_1_data["data"] == ""
    with open(file_1_data["location"]) as f:
        assert f.read() == file_1_content

    file_2_data = data["file_2"]["value"]
    file_2_content = tw.dedent(
        f"""
        Copy of {file_1_data["location"]}:
        --------------------------
        {file_1_content}
        --------------------------
        Copied {date.today().isoformat()}
        """
    ).strip()

    assert file_2_data["location"] == os.path.realpath(tmpdir + "/test-2.txt.backup")
    assert file_2_data["data"] == ""
    with open(file_2_data["location"]) as f:
        assert f.read() == file_2_content

    assert "file_3" not in data

    async for _ in resource_graph.refresh():
        pass

    keys = ["file_1", "file_2"]

    for key in keys:
        file_data = data[key]["value"]
        with open(file_data["location"]) as f:
            assert f.read() == file_data["data"]

    plan_post_update_2 = await migrator.plan(session_2, resource_graph)
    assert plan_post_update_2.is_empty()


@pytest.mark.asyncio
async def test_update(tmpdir, resource_session, migrator, executor):

    current_rg = await setup_state(files_test_2, tmpdir)
    keys = list(current_rg.graph.nodes)

    files_test_3(resource_session, tmpdir)

    plan = await migrator.plan(resource_session, current_rg)

    task_graph = plan.task_graph.task_graph
    resource_graph = plan.task_graph.resource_graph

    assert {node.key for node in plan.nodes} == set(keys)

    assert set(task_graph.nodes) == {
        "file_1:input",
        "file_1:task:rename_file",
        "file_1:output",
        "file_1:state",
        "file_2:input",
        "file_2:task:update_file",
        "file_2:output",
        "file_2:state",
    }

    assert_depends("file_2:input", "file_1:output", task_graph)

    exec_info = await executor.execute_async(plan.task_graph)

    assert exec_info.is_success()

    data = resource_graph.graph.nodes

    assert set(data) == set(keys)

    file_1_data = data["file_1"]["value"]
    file_1_content = "Hello, world! 2"

    assert file_1_data["location"] == os.path.realpath(tmpdir + "/test-3.txt")
    assert file_1_data["data"] == ""
    with open(file_1_data["location"]) as f:
        assert f.read() == file_1_content

    file_2_data = data["file_2"]["value"]
    file_2_content = tw.dedent(
        f"""
        Copy of {file_1_data["location"]}:
        --------------------------
        {file_1_content}
        --------------------------
        Copied {date.today().isoformat()}
        """
    ).strip()

    assert file_2_data["location"] == os.path.realpath(tmpdir + "/test-2.txt.backup")
    assert file_2_data["data"] == ""
    with open(file_2_data["location"]) as f:
        assert f.read() == file_2_content

    async for _ in resource_graph.refresh():
        pass

    keys = ["file_1", "file_2"]

    for key in keys:
        file_data = data[key]["value"]
        with open(file_data["location"]) as f:
            assert f.read() == file_data["data"]

    plan_post_update = await migrator.plan(resource_session, resource_graph)
    assert plan_post_update.is_empty()


@pytest.mark.asyncio
async def test_teardown(tmpdir, resource_session, migrator, executor):

    resource_graph = await setup_state(files_test_2, tmpdir)
    keys = ["file_1", "file_2"]

    session_3 = resource_session

    plan_3 = await migrator.plan(session_3, resource_graph)

    task_graph = plan_3.task_graph.task_graph
    resource_graph = plan_3.task_graph.resource_graph

    nodes_by_key = {node.key: node for node in plan_3.nodes}

    assert set(nodes_by_key) == set(keys)

    assert set(task_graph.nodes) == {
        f"{first}:{second}"
        for first in ["file_1", "file_2"]
        for second in [
            "input",
            "task:delete_file",
            "output",
        ]
    }

    assert_depends("file_1:input", "file_2:output", task_graph)

    exec_info = await executor.execute_async(plan_3.task_graph)

    assert exec_info.is_success()

    tasks_by_status = exec_info.tasks_by_status()

    assert set(tasks_by_status) == {st.TaskStatus.SUCCESS}

    assert len(tasks_by_status[st.TaskStatus.SUCCESS]) == len(task_graph.nodes)

    assert not resource_graph.graph.nodes

    assert not os.listdir(tmpdir)


@pytest.mark.asyncio
async def test_plan_chain(tmpdir, resource_session, migrator, executor):

    session = resource_session
    files_test_1(session, tmpdir)

    plan = await migrator.plan(session)

    rg = plan.state_graph

    session_2 = st.create_resource_session()
    files_test_2(session_2, tmpdir)

    plan_2 = (await plan.plan(session_2)).second

    session_3 = st.create_resource_session()

    # Equivalent
    # plan_3 = await migrator.plan(session_3, rg, plan_2.task_graph.output_session)
    plan_3 = (await plan_2.plan(session_3)).second

    exec_info = await executor.execute_async(plan.task_graph)

    assert exec_info.is_success()

    assert set(rg.keys()) == {"file_1", "file_2", "file_3"}

    assert set(os.listdir(tmpdir)) == {
        "test-1.txt",
        "test-1.txt.backup",
        "test-1.txt.backup.json",
    }

    exec_info_2 = await executor.execute_async(plan_2.task_graph)

    assert exec_info_2.is_success()

    assert set(rg.keys()) == {"file_1", "file_2"}

    assert set(os.listdir(tmpdir)) == {"test-2.txt", "test-2.txt.backup"}

    exec_info_3 = await executor.execute_async(plan_3.task_graph)

    assert exec_info_3.is_success()

    assert os.listdir(tmpdir) == rg.keys() == []


@pytest.mark.asyncio
async def test_compound_plan(tmpdir, resource_session, migrator, executor):

    session = resource_session
    files_test_2(session, tmpdir)

    plan = await migrator.plan(session)

    rg = plan.state_graph

    session_2 = st.create_resource_session()
    files_test_3(session_2, tmpdir)

    plan_2 = await plan.plan(session_2)

    exec_info = await executor.execute_async(plan_2.task_graph)

    assert exec_info.is_success()

    assert set(rg.keys()) == {"file_1", "file_2"}

    assert set(os.listdir(tmpdir)) == {"test-3.txt", "test-2.txt.backup"}

    async for _ in rg.refresh():
        pass

    finished_plan = await migrator.plan(session_2, rg)

    assert finished_plan.is_empty()
