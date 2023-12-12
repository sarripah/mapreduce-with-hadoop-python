from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
#split by ,
columns = 'show_id,type,title,director,cast,country,release_year,rating,duration,listed_in'.split(',')
class Director(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_director,
                  reducer=self.reducer_count_director)
        ]
    #Mapper function 
    def mapper_get_director(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           director=diction['director']
           #outputing as key value pairs
           yield director, 1
    #Reducer function
    def reducer_count_director(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    Director.run()