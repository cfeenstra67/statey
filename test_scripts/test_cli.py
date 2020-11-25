from statey.cli.__main__ import cli

ctx = cli.make_context('statey.cli', ['plan', '--task-dag', 'test_pulumi_resource', 'up'])
with ctx:
    result = cli.invoke(ctx)
