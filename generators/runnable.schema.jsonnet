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
    'in.sha256': {
      oneOf: [
        {
          type: 'string',
          minLength: 64,
          maxLength: 64,
        },
        {
          type: 'object',
          additionalProperties: {
            type: 'string',
            minLength: 64,
            maxLength: 64,
          },
          examples: [
            {
              myText: 'b24b59e210cefd4d964413ac555e4573470909f8aa3d16b6b9a65ddb04490b69',
              'foo2.csv': 'ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9',
              SourceCode: '1c8a656566dacfaaaece7b5e3e52a59faca12d3803443a1a56fdd5a9a7b8e347',
            },
          ],
        },
      ],
    },
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
