INSERT INTO blocks_to_invalidate_doubled_tt(block_number, refetched)
SELECT DISTINCT t2.block_number, false
FROM transactions t2,
(
  SELECT DISTINCT a.transaction_hash FROM
  (
      SELECT t.transaction_hash
      , t.to_address_hash
      , t.from_address_hash
      , t.amount
      , t.token_id
      , count(*) FROM token_transfers t
     GROUP BY t.transaction_hash, t.to_address_hash, t.from_address_hash, t.amount, t.token_id
     HAVING count(*) > 1
   ) a
) t3
WHERE t2.hash=t3.transaction_hash;