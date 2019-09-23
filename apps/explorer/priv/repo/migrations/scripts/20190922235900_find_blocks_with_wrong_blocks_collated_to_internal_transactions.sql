INSERT INTO blocks_to_invalidate_wrong_int_txs_collation(block_number, refetched)
SELECT DISTINCT block_number, false
FROM internal_transactions
WHERE transaction_hash IN (
	SELECT a.transaction_hash
	FROM (
		SELECT transaction_hash, count(distinct block_number)
		FROM internal_transactions
		GROUP BY transaction_hash HAVING count(distinct block_number) > 1
	) a
);