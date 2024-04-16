from mrjob.job import MRJob, MRStep


class Job(MRJob):

    def steps(self):
        return [MRStep(),
                MRStep(),
                MRSwtep(),
                MRStep()]


if __name__ == "__main__":
    Job().run()
