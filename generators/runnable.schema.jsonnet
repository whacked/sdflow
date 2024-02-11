{
  // '$schema': 'http://json-schema.org/draft-07/schema#',
  type: 'object',
  properties: {
    name: { type: 'string' },
    'in': { type: 'string' },
    out: { type: 'string' },
    pre: { type: 'string' },
    post: { type: 'string' },
    run: { type: 'string' },
    notify: {
      type: 'object',
      additionalProperties: true,
    },
  },
  required: ['run'],
  additionalProperties: false,
}
