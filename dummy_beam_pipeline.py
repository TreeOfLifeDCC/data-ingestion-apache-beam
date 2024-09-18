import apache_beam as beam

inputs = [
    'First_element',
    'Second_element',
    'Last_element'
]

print('Initializing beam pipeline')
with beam.Pipeline() as p:
    (p
     | 'Read data' >> beam.Create(inputs)
     | 'ProcessData' >> beam.Map(lambda x: x.upper())
     | 'Print data' >> beam.Map(lambda line: print(line))
    )
print('Finishing beam pipeline')
