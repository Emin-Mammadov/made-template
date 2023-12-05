import os
import subprocess
import unittest

class TestDataPipeline(unittest.TestCase):
    def test_data_pipeline(self):
        # Run your pipeline
        subprocess.run(['bash', './project/pipeline.sh'])

        # Validate output files
        bitcoin_output = './data/bitcoin_data.sqlite'
        sp500_output = './data/sp500_data.sqlite'

        self.assertTrue(os.path.exists(bitcoin_output), "Bitcoin data file not found")
        self.assertTrue(os.path.exists(sp500_output), "S&P 500 data file not found")

if __name__ == "__main__":
    # Run system-level test
    unittest.main()
