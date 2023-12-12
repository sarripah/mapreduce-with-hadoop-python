from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
#split by ,
columns = 'show_id,type,title,director,cast,country,release_year,rating,duration,listed_in '.split(',')
class Type(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_type,
                  reducer=self.reducer_count_type)
        ]
    #Mapper function 
    def mapper_get_type(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           type=diction['type']
           #outputing as key value pairs
           yield type, 1
    #Reducer function
    def reducer_count_type(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    Type.run()