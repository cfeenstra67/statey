import pytest

import statey as st


@pytest.mark.parametrize('annotation, name', [
	(int, 'IntField'),
	(str, 'StrField'),
	(float, 'FloatField'),
	(bool, 'BoolField')
])
def test_field_annotation_get(annotation, name):
	from statey.schema.field import _FieldWithModifiers

	field = st.Field[annotation]
	assert isinstance(field, _FieldWithModifiers)
	assert field.field_cls.__name__ == name
	assert field.defaults == {'annotation': annotation}
