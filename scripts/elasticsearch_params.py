SPANISH_STOP_FILTER = 'name_stop_spanish'
LOWCASE_ASCII_NORMALIZER = 'lowcase_ascii_normalizer'
NAME_ANALYZER = 'name_analyzer'
NAME_ANALYZER_SYNONYMS = 'name_analyzer_synonyms'
ENTITY_NAME_SYNONYMS_FILTER = 'entity_name_synonyms'


def build_synonyms(original, synonyms):
    return '{} => {}'.format(original, ', '.join([original] + synonyms))


DEFAULT_SETTINGS = {
    'analysis': {
        'filter': {
            SPANISH_STOP_FILTER: {
                'type': 'stop',
                'stopwords': ['la', 'las', 'el', 'los', 'de', 'del', 'y', 'e']
            },
            ENTITY_NAME_SYNONYMS_FILTER: {
                'type': 'synonym',
                'synonyms': [
                    build_synonyms('ciudad autonoma buenos aires', [
                        'caba', 'c.a.b.a.', 'capital', 'capital federal'
                    ]),
                    build_synonyms('buenos aires', [
                        'gba', 'bsas', 'bs.as.'
                    ])
                ]
            }
        },
        'normalizer': {
            LOWCASE_ASCII_NORMALIZER: {
                'type': 'custom',
                'filter': [
                    'lowercase',
                    'asciifolding'
                ]
            }
        },
        'analyzer': {
            NAME_ANALYZER: {
                'type': 'custom',
                'tokenizer': 'whitespace',
                'filter': [
                    'lowercase',
                    'asciifolding',
                    SPANISH_STOP_FILTER
                ]
            },
            NAME_ANALYZER_SYNONYMS: {
                'type': 'custom',
                'tokenizer': 'whitespace',
                'filter': [
                    'lowercase',
                    'asciifolding',
                    SPANISH_STOP_FILTER,
                    ENTITY_NAME_SYNONYMS_FILTER
                ]
            }
        }
    }
}