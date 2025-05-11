{
  // '$schema': 'http://json-schema.org/draft-07/schema#',
  type: 'object',
  properties: {
    name: { type: 'string' },
    'in': {
      oneOf: [
        {
          type: 'string',
        },
        {
          // multiple inputs
          type: 'array',
          items: { type: 'string' },
          examples: [
            ['src1.txt', 'foo2.csv', 'bar3.cpp'],
          ],
        },
        {
          // mapped inputs
          type: 'object',
          additionalProperties: {
            type: 'string',
          },
          examples: [
            {
              myText: 'src1.txt',
              csv_file: 'foo2.csv',
              SourceCode: 'bar3.cpp',
            },
          ],
        },
      ],
    },
    'in.sha256': { type: 'string' },
    out: { type: 'string' },
    'out.sha256': {
      type: 'string',
      minLength: 64,
      maxLength: 64,
    },
    pre: { type: 'string' },
    post: { type: 'string' },
    run: { type: 'string' },
    notify: {
      type: 'object',
      additionalProperties: true,
    },
  },
  required: [],
  additionalProperties: false,
}
