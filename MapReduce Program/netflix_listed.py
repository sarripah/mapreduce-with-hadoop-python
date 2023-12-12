from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
#split by ,
columns = 'show_id,type,title,director,cast,country,release_year,rating,duration,listed_in'.split(',')
class Listed(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_listed,
                  reducer=self.reducer_count_listed)
        ]
    #Mapper function 
    def mapper_get_listed(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           listed=diction['listed_in']
           #outputing as key value pairs
           yield listed, 1
    #Reducer function
    def reducer_count_listed(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    Listed.run()