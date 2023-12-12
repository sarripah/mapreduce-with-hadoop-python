from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
#split by ,
columns = 'show_id,type,title,director,cast,country,release_year,rating,duration,listed_in'.split(',')
class Rating(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_rating,
                  reducer=self.reducer_count_rating)
        ]
    #Mapper function 
    def mapper_get_rating(self, _, line):
       reader = csv.reader([line])
       for row in reader:
           zipped=zip(columns,row)
           diction=dict(zipped)
           rating=diction['rating']
           #outputing as key value pairs
           yield rating, 1
    #Reducer function
    def reducer_count_rating(self, key, values):
       yield key, sum(values)
if __name__ == "__main__":
    Rating.run()