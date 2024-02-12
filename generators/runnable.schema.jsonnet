{
  // '$schema': 'http://json-schema.org/draft-07/schema#',
  type: 'object',
  properties: {
    name: { type: 'string' },
    'in': { type: 'string' },
    'in.sha256': { type: 'string' },
    out: { type: 'string' },
    'out.sha256': { type: 'string' },
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
