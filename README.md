# gaft
- A Raft implementation in golang



## Raft Explantion

### States of given node at any instant time
1. Leader
2. Candidate
3. Follower

### LeaderNode resposiblities
- Talk with actual client
- Sends heartBeats to followers every Xms to ensure that it's authority as leader is maintained
- On client persistence request, send logs to majority of follower nodes so that it can mark it as commited

