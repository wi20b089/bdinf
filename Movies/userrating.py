from mrjob.job import MRJob
class MRMovieCounter(MRJob):

    
    def mapper(self, _, rater):
        (garbage1, title, garbage2) = rater.partition(",")
        yield (title, 1)
    def reducer(self, key, values):
        yield "Sum of Datasets:", sum(values)-1

if __name__ == "__main__":
    MRMovieCounter.run()
