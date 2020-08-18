import textwrap as tw

import statey as st

from statey.lib.os import File, FileType


@st.function
def create_report(file: FileType) -> str:
	return tw.dedent(f"""
	Report on {file['location']}:
	Mode: {file['stat']['mode']}
	Ino: {file['stat']['ino']}
	Dev: {file['stat']['dev']}
	NLink: {file['stat']['nlink']}
	UID: {file['stat']['uid']}
	GID: {file['stat']['gid']}
	Size: {file['stat']['size']}
	Mtime: {file['stat']['mtime']}
	Ctime: {file['stat']['ctime']}
	Data:
	{tw.indent(file['data'], '	')}
	""")


@st.declarative
def create_files(session, start_path: str):
	data = 'Hello, world! Four score and seven years ago...'
	file1 = File(
		location=start_path,
		data=data
	)
	file2 = File(
		location=file1.location + '.report',
		data=create_report(file1)
	)
	file3 = File(
		location=file2.location + '.report',
		data=create_report(file2)
	)


def session():
	session = st.create_resource_session()
	create_files(session, './tmp.txt')
	return session
