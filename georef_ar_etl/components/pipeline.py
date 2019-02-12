class Pipeline:
    def __init__(self, steps):
        self._steps = steps

    def run(self, data, context):
        result = data
        for step in self._steps:
            result = step(result, context)

        return result
