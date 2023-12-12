from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
#split by ,
columns = 'show_id,type,title,director,cast,country,release_year,rating,duration,listed_in'.split(',')
class Year(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_year,
                  reducer=self.reducer_count_year)
        ]
    #Mapper function 
    def mapper_get_year(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           year=diction['release_year']
           #outputing as key value pairs
           yield year, 1
    #Reducer function
    def reducer_count_year(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    Year.run()