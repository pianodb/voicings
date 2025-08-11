Starting tournament merge...
Found 1143 parquet files to process
Initial aggregation: 100%|████████████████████████████████████████████████████| 1143/1143 [04:19<00:00,  4.40it/s]
Initial aggregation took: 259.78 seconds
Created 1143 chunks for tournament
Round 1: 572 tables remaining (took 27.26s)
Round 2: 286 tables remaining (took 11.25s)
Round 3: 143 tables remaining (took 8.38s)
Round 4: 72 tables remaining (took 5.76s)
Round 5: 36 tables remaining (took 4.88s)
Round 6: 18 tables remaining (took 3.39s)
Round 7: 9 tables remaining (took 2.60s)
Round 8: 5 tables remaining (took 1.76s)
Round 9: 3 tables remaining (took 1.29s)
Round 10: 2 tables remaining (took 1.00s)
Round 11: 1 tables remaining (took 0.90s)
Tournament merge took: 68.49 seconds
Total tournament time: 328.86 seconds
Tournament done.
File write took: 1.63 seconds
Starting refuse bin processing...
Processing refuse (uncommon fragments): 100%|█████████████████████████████████| 1143/1143 [05:07<00:00,  3.72it/s]
Refuse bin processing took: 307.20 seconds
Overall execution time: 962.30 seconds


Starting with 915 chunks
Using 915 provided chunks for tournament
Round 1: 458 tables remaining (took 91.15s)
Round 2: 229 tables remaining (took 130.52s)
Round 3: 115 tables remaining (took 138.59s)
Round 4: 58 tables remaining (took 164.79s)
Round 5: 29 tables remaining (took 180.94s)
Round 6: 15 tables remaining (took 171.41s)
Round 7: 8 tables remaining (took 186.82s)
Round 8: 4 tables remaining (took 254.28s)
Round 9: 2 tables remaining (took 510.53s)
FAILED FAILED FAILED

Starting cyclic aggregation tournament
Iteration 0
Starting with 274222204 rows
Starting with 10 chunks
Chunking done.
Finding non-unique values: 100%|██████████████████████████████████████████████████| 10/10 [00:54<00:00,  5.48s/it]
Duplicate finding done.
Siphoning dataframe into duplicate/nonduplicate: 100%|████████████████████████████| 10/10 [02:58<00:00, 17.84s/it]
Dataframe siphoned into duplicate/nonduplicate.
Written to file.
Done shuffling.
Iteration 1
Starting with 156009052 rows
Starting with 6 chunks
Chunking done.
Finding non-unique values: 100%|████████████████████████████████████████████████████| 6/6 [00:35<00:00,  5.85s/it]
Duplicate finding done.
Siphoning dataframe into duplicate/nonduplicate: 100%|██████████████████████████████| 6/6 [01:36<00:00, 16.08s/it]
Dataframe siphoned into duplicate/nonduplicate.
Written to file.
Done shuffling.
Iteration 2
Starting with 136818779 rows
Starting with 5 chunks
Chunking done.
Finding non-unique values: 100%|████████████████████████████████████████████████████| 5/5 [00:24<00:00,  4.80s/it]
Duplicate finding done.
Siphoning dataframe into duplicate/nonduplicate: 100%|██████████████████████████████| 5/5 [00:54<00:00, 10.87s/it]
Dataframe siphoned into duplicate/nonduplicate.
Written to file.
Done shuffling.
Iteration 3
Starting with 127843454 rows
Starting with 5 chunks
Chunking done.
Finding non-unique values: 100%|████████████████████████████████████████████████████| 5/5 [00:32<00:00,  6.51s/it]
Duplicate finding done.
Siphoning dataframe into duplicate/nonduplicate: 100%|██████████████████████████████| 5/5 [01:12<00:00, 14.42s/it]
Dataframe siphoned into duplicate/nonduplicate.
Written to file.
Done shuffling.
Iteration 4
Starting with 122241804 rows
Starting with 5 chunks
Chunking done.
Finding non-unique values: 100%|████████████████████████████████████████████████████| 5/5 [00:34<00:00,  6.84s/it]
Duplicate finding done.
Siphoning dataframe into duplicate/nonduplicate: 100%|██████████████████████████████| 5/5 [01:47<00:00, 21.48s/it]
Dataframe siphoned into duplicate/nonduplicate.
Written to file.
Done shuffling.

Starting cyclic aggregation tournament
Iteration 0
Starting with 118322602 rows
Starting with 1 chunks
Chunking done.
Finding non-unique values: 100%|███████████████████████████████████████████████████| 1/1 [02:31<00:00, 151.89s/it]
Duplicate finding done.
Siphoning dataframe into duplicate/nonduplicate: 100%|█████████████████████████████| 1/1 [05:22<00:00, 322.76s/it]
Dataframe siphoned into duplicate/nonduplicate.
Written to file.
Done shuffling.
We processed the entire data in one batch; stopping.