import json


class JSONArrayPlaceholder:
    PLACEHOLDER = '@@JSON_ARRAY_PLACEHOLDER@@'


def default_json_encode(obj):
    if isinstance(obj, JSONArrayPlaceholder):
        return JSONArrayPlaceholder.PLACEHOLDER

    raise TypeError(type(obj))


class JSONStreamWriter:
    def __init__(self, fp, template=None, bufsize=1024, **kwargs):
        if template is None:
            template = JSONArrayPlaceholder()

        template_str = json.dumps(template, default=default_json_encode,
                                  **kwargs)

        template_str_parts = template_str.split('"{}"'.format(
            JSONArrayPlaceholder.PLACEHOLDER))

        if len(template_str_parts) != 2:
            raise ValueError('Template object must contain exactly one '
                             'JSONArrayPlaceholder object.')

        self._fp = fp
        self._first = True
        self._objects = []
        self._bufsize = bufsize
        self._kwargs = kwargs
        self._template_part_a = template_str_parts[0]
        self._template_part_b = template_str_parts[1]

    def append(self, obj):
        self._objects.append(obj)

        if len(self._objects) == self._bufsize:
            for o in self._objects:
                self._write_object(o)

            self._objects.clear()

    def _write_object(self, obj):
        if self._first:
            self._first = False
        else:
            self._fp.write(',')

        json.dump(obj, self._fp, **self._kwargs)

    def __enter__(self):
        self._fp.write(self._template_part_a)
        self._fp.write('[')
        return self

    def __exit__(self, *args):
        for o in self._objects:
            self._write_object(o)
        self._objects.clear()

        self._fp.write(']')
        self._fp.write(self._template_part_b)
