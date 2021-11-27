"""
 Xlen test_stream
(integer) 177
Alternative:
XINFO STREAM mystream
 1) length
XINFO GROUPS mystream -> pending
if diff is big -> scale out
if ~ no diff   -> scale in
"""
