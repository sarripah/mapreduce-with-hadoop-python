from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

# Split by ,
columns = 'show_id,type,title,director,cast,country,release_year,rating,duration,listed_in'.split(',')

class DurationAverage(MRJob):
    def mapper(self, _, line):
        reader = csv.reader([line])
        for row in reader:
            duration = row[8]
            try:
                duration = float(duration)
                yield None, (duration, 1)
            except ValueError:
                pass

    def reducer(self, _, values):
        total_duration = 0
        total_count = 0

        for duration, count in values:
            total_duration += duration
            total_count += count

        average_duration = total_duration / total_count if total_count > 0 else 0

        yield None, average_duration

if __name__ == '__main__':
    DurationAverage.run()