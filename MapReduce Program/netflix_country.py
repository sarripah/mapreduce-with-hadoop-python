from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
#split by ,
columns = 'show_id,type,title,director,cast,country,release_year,rating,duration,listed_in'.split(',')
class Country(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_country,
                  reducer=self.reducer_count_country)
        ]
    #Mapper function 
    def mapper_get_country(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           country=diction['country']
           #outputing as key value pairs
           yield country, 1
    #Reducer function
    def reducer_count_country(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    Country.run()