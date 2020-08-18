import statey as st
from statey.cli.graph_utils import ascii_dag

from statey.lib import os as sos


task_session = st.create_task_session()

@st.task.new
async def nothing(x: int) -> int:
	return x


obj = task_session['input'] << st.Object({'location': './a.b', 'data': ''}, sos.FileConfigType)

task1 = task_session['task1'] << sos.FileMachine.remove_file(st.Object('Fuck'))

task2 = task_session['task2'] << sos.FileMachine.set_file(st.join(obj, task1))

print(ascii_dag(task_session.dependency_graph()))
