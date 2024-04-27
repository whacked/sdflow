{
  '$schema': 'http://json-schema.org/draft-07/schema#',
  type: 'object',
  patternProperties: {
    '.*': {
      oneOf: [
        {
          // single variable
          type: 'string',
        },
        {
          // dependencies
          type: 'array',
          items: { type: 'string' },
        },
        (import './runnable.schema.jsonnet'),
      ],
    },
  },
  additionalProperties: false,
}
