-- SQL dialect probe corpus. See corpus.py for how this file is read.
--
-- Each probe is a `-- probe: <id> <kind>` line followed by one statement on one line.
-- Kinds: common, duckdb, spark (feature probes: does the statement run?) and
-- semantics (the first value of the first result column is also compared).
-- {DATA} stands for the directory that holds the probe tables as files.

-- ---- Common SQL: statements that more than one reference engine accepts.

-- probe: select_where common
SELECT a, b FROM t WHERE a > 2;

-- probe: order_limit_offset common
SELECT a FROM t ORDER BY a DESC LIMIT 2 OFFSET 1;

-- probe: distinct common
SELECT DISTINCT b FROM t;

-- probe: group_having common
SELECT b, COUNT(*) AS n FROM t GROUP BY b HAVING COUNT(*) > 1;

-- probe: group_by_position common
SELECT b, SUM(a) AS s FROM t GROUP BY 1;

-- probe: group_by_select_alias common
SELECT b AS bb, SUM(a) AS s FROM t GROUP BY bb;

-- probe: case_when common
SELECT CASE WHEN a > 2 THEN 'hi' ELSE 'lo' END AS k FROM t;

-- probe: coalesce_nullif common
SELECT COALESCE(a, 0) AS x, NULLIF(b, 'x') AS y FROM t;

-- probe: in_between_like common
SELECT * FROM t WHERE b IN ('x', 'y') AND a BETWEEN 1 AND 4 AND b LIKE 'x%';

-- probe: is_distinct_from common
SELECT a IS DISTINCT FROM 3 AS v FROM t;

-- probe: ilike common
SELECT * FROM t WHERE b ILIKE 'X';

-- probe: cte common
WITH s AS (SELECT * FROM t WHERE a > 1) SELECT COUNT(*) AS n FROM s;

-- probe: cte_recursive common
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT SUM(n) AS s FROM r;

-- probe: union_all common
SELECT a FROM t UNION ALL SELECT w FROM u;

-- probe: union_distinct common
SELECT b FROM t UNION SELECT b FROM u;

-- probe: intersect common
SELECT b FROM t INTERSECT SELECT b FROM u;

-- probe: except common
SELECT b FROM t EXCEPT SELECT b FROM u;

-- probe: intersect_all common
SELECT b FROM t INTERSECT ALL SELECT b FROM u;

-- probe: except_all common
SELECT b FROM t EXCEPT ALL SELECT b FROM u;

-- probe: values common
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y);

-- probe: scalar_subquery common
SELECT a, (SELECT MAX(w) FROM u) AS m FROM t;

-- probe: correlated_scalar common
SELECT a, (SELECT w FROM u WHERE u.b = t.b) AS w FROM t;

-- probe: exists common
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.b = t.b);

-- probe: not_in_subquery common
SELECT * FROM t WHERE b NOT IN (SELECT b FROM u);

-- probe: any_subquery common
SELECT * FROM t WHERE a >= ANY (SELECT w / 10 FROM u);

-- probe: join_using common
SELECT * FROM t JOIN u USING (b);

-- probe: left_join common
SELECT t.a, u.w FROM t LEFT JOIN u ON t.b = u.b;

-- probe: right_join common
SELECT t.a, u.w FROM t RIGHT JOIN u ON t.b = u.b;

-- probe: full_join common
SELECT t.a, u.w FROM t FULL OUTER JOIN u ON t.b = u.b;

-- probe: cross_join common
SELECT COUNT(*) AS n FROM t CROSS JOIN u;

-- probe: natural_join common
SELECT * FROM t NATURAL JOIN u;

-- probe: non_equi_join common
SELECT t.a, u.w FROM t JOIN u ON t.a * 10 < u.w;

-- probe: join_or_condition common
SELECT t.a, u.w FROM t JOIN u ON t.b = u.b OR t.a = 3;

-- probe: self_join common
SELECT x.name, y.name AS peer FROM emp x JOIN emp y ON x.dept = y.dept AND x.name < y.name;

-- probe: lateral_subquery common
SELECT t.a, s.m FROM t, LATERAL (SELECT MAX(w) AS m FROM u WHERE u.b = t.b) s;

-- probe: rollup common
SELECT dept, year, SUM(salary) AS s FROM emp GROUP BY ROLLUP (dept, year);

-- probe: cube common
SELECT dept, year, SUM(salary) AS s FROM emp GROUP BY CUBE (dept, year);

-- probe: grouping_sets common
SELECT dept, year, SUM(salary) AS s FROM emp GROUP BY GROUPING SETS ((dept), (year), ());

-- probe: grouping_fn common
SELECT dept, GROUPING(dept) AS g, SUM(salary) AS s FROM emp GROUP BY ROLLUP (dept);

-- probe: agg_filter common
SELECT COUNT(*) FILTER (WHERE a > 2) AS n FROM t;

-- probe: count_distinct common
SELECT COUNT(DISTINCT b) AS n FROM t;

-- probe: window_rank common
SELECT name, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS r FROM emp;

-- probe: window_rows_frame common
SELECT name, SUM(salary) OVER (ORDER BY name ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM emp;

-- probe: window_range_frame common
SELECT name, SUM(salary) OVER (ORDER BY salary RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM emp;

-- probe: window_lag_lead common
SELECT name, LAG(salary) OVER (ORDER BY name) AS p, LEAD(salary, 1, 0) OVER (ORDER BY name) AS n FROM emp;

-- probe: window_first_last common
SELECT name, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary) AS f, LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS l FROM emp;

-- probe: window_ntile_dist common
SELECT name, NTILE(2) OVER (ORDER BY salary) AS t, PERCENT_RANK() OVER (ORDER BY salary) AS p, CUME_DIST() OVER (ORDER BY salary) AS c FROM emp;

-- probe: named_window common
SELECT name, SUM(salary) OVER w AS s FROM emp WINDOW w AS (PARTITION BY dept);

-- probe: qualify common
SELECT * FROM emp QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1;

-- probe: order_nulls_first common
SELECT a FROM t ORDER BY a NULLS FIRST;

-- probe: fetch_first common
SELECT a FROM t ORDER BY a FETCH FIRST 2 ROWS ONLY;

-- probe: cast_common common
SELECT CAST(a AS VARCHAR) AS s, CAST('2026-01-02' AS DATE) AS d, CAST(c AS INTEGER) AS i, CAST(a AS DECIMAL(10, 2)) AS m FROM t;

-- probe: try_cast common
SELECT TRY_CAST('x' AS INTEGER) AS v;

-- probe: double_colon_cast common
SELECT a::VARCHAR AS s FROM t;

-- probe: string_fns common
SELECT UPPER(b) AS u, LOWER(b) AS l, LENGTH(b) AS n, TRIM('  x ') AS tr, REPLACE(b, 'x', 'X') AS r, CONCAT(b, '!') AS c, LPAD(b, 3, '.') AS p, LEFT(b, 1) AS lf, REVERSE(b) AS rv FROM t;

-- probe: substring_from_for common
SELECT SUBSTRING(b FROM 1 FOR 1) AS s FROM t;

-- probe: position_in common
SELECT POSITION('x' IN b) AS p FROM t;

-- probe: regexp_replace common
SELECT REGEXP_REPLACE(b, 'x', 'X') AS r FROM t;

-- probe: regexp_extract common
SELECT REGEXP_EXTRACT(name, '(a)(n)', 1) AS r FROM emp;

-- probe: math_fns common
SELECT ABS(-a) AS ab, ROUND(c, 1) AS r, FLOOR(c) AS f, CEIL(c) AS ce, POWER(a, 2) AS p, SQRT(a) AS sq, LN(a) AS ln, EXP(1) AS e, MOD(a, 2) AS m, a % 2 AS m2, GREATEST(a, 3) AS g, LEAST(a, 3) AS l, SIGN(a - 3) AS s FROM t;

-- probe: extract_date_trunc common
SELECT EXTRACT(YEAR FROM d) AS y, DATE_TRUNC('month', d) AS m FROM t;

-- probe: date_plus_interval common
SELECT d + INTERVAL 1 DAY AS d1, ts - INTERVAL 1 HOUR AS t1 FROM t;

-- probe: current_date common
SELECT CURRENT_DATE AS d, CURRENT_TIMESTAMP AS ts;

-- probe: agg_stats common
SELECT AVG(salary) AS a, STDDEV(salary) AS s, VARIANCE(salary) AS v, MEDIAN(salary) AS m, CORR(salary, year) AS c FROM emp;

-- probe: percentile_cont_within common
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS p FROM emp;

-- probe: bool_and_or common
SELECT BOOL_AND(flag) AS a, BOOL_OR(flag) AS o FROM t;

-- probe: approx_count_distinct common
SELECT APPROX_COUNT_DISTINCT(b) AS n FROM t;

-- probe: max_by_min_by common
SELECT MAX_BY(name, salary) AS hi, MIN_BY(name, salary) AS lo FROM emp;

-- probe: count_if common
SELECT COUNT_IF(a > 2) AS n FROM t;

-- probe: any_value common
SELECT dept, ANY_VALUE(name) AS n FROM emp GROUP BY dept;

-- probe: array_agg_ordered common
SELECT dept, ARRAY_AGG(name ORDER BY name) AS names FROM emp GROUP BY dept;

-- probe: lateral_column_alias common
SELECT a * 2 AS x, x + 1 AS y FROM t;

-- probe: order_by_unselected common
SELECT b FROM t ORDER BY a;

-- probe: struct_field_access common
SELECT st.p AS p, st.q AS q FROM nested;

-- probe: star_qualified common
SELECT t.*, u.w FROM t JOIN u ON t.b = u.b;

-- probe: create_temp_view common
CREATE TEMPORARY VIEW v1 AS SELECT 1 AS x;

-- probe: explain common
EXPLAIN SELECT * FROM t WHERE a > 1;

-- probe: describe_table common
DESCRIBE t;

-- probe: show_tables common
SHOW TABLES;

-- probe: information_schema common
SELECT column_name FROM information_schema.columns WHERE table_name = 't';

-- probe: md5_hash common
SELECT MD5(b) AS h FROM t;

-- probe: typeof common
SELECT TYPEOF(a) AS ty FROM t;

-- probe: unnest_select common
SELECT id, UNNEST(arr) AS x FROM nested;

-- probe: pivot_std common
SELECT * FROM (SELECT dept, year, salary FROM emp) PIVOT (SUM(salary) FOR dept IN ('eng', 'ops'));

-- probe: unpivot_std common
SELECT * FROM (SELECT 1 AS a, 2 AS b) UNPIVOT (v FOR k IN (a, b));

-- ---- DuckDB idioms.

-- probe: from_first duckdb
FROM t SELECT a;

-- probe: from_only duckdb
FROM t;

-- probe: file_as_table duckdb
SELECT COUNT(*) AS n FROM '{DATA}/t.parquet';

-- probe: read_parquet duckdb
SELECT COUNT(*) AS n FROM read_parquet('{DATA}/t.parquet');

-- probe: read_parquet_glob duckdb
SELECT COUNT(*) AS n FROM read_parquet('{DATA}/glob/*.parquet');

-- probe: read_csv duckdb
SELECT * FROM read_csv('{DATA}/t_csv.csv');

-- probe: read_csv_option duckdb
SELECT * FROM read_csv('{DATA}/t_csv.csv', header = true);

-- probe: select_exclude duckdb
SELECT * EXCLUDE (c) FROM t;

-- probe: select_replace duckdb
SELECT * REPLACE (a * 2 AS a) FROM t;

-- probe: columns_regex duckdb
SELECT COLUMNS('a|c') FROM t;

-- probe: columns_agg duckdb
SELECT MAX(COLUMNS('a|c')) FROM t;

-- probe: group_by_all duckdb
SELECT b, SUM(a) AS s FROM t GROUP BY ALL;

-- probe: order_by_all duckdb
SELECT b, a FROM t ORDER BY ALL;

-- probe: distinct_on duckdb
SELECT DISTINCT ON (b) b, a FROM t ORDER BY b, a;

-- probe: list_literal duckdb
SELECT [1, 2, 3] AS l;

-- probe: struct_literal duckdb
SELECT {'p': 1, 'q': 'a'} AS s;

-- probe: list_comprehension duckdb
SELECT [x * 2 FOR x IN arr] AS l FROM nested;

-- probe: list_transform_lambda duckdb
SELECT list_transform(arr, x -> x + 1) AS l FROM nested;

-- probe: list_transform_lambda_kw duckdb
SELECT list_transform(arr, lambda x: x + 1) AS l FROM nested;

-- probe: list_filter_lambda duckdb
SELECT list_filter(arr, x -> x > 1) AS l FROM nested;

-- probe: list_fns duckdb
SELECT list_contains(arr, 1) AS c, len(arr) AS n, list_sum(arr) AS s, list_sort(arr) AS so FROM nested;

-- probe: string_slice duckdb
SELECT name[1:2] AS s FROM emp;

-- probe: unnest_struct duckdb
SELECT UNNEST(st) FROM nested;

-- probe: pivot_duckdb duckdb
PIVOT emp ON dept USING SUM(salary) GROUP BY year;

-- probe: unpivot_duckdb duckdb
UNPIVOT (SELECT 1 AS a, 2 AS b) ON a, b INTO NAME k VALUE v;

-- probe: asof_join duckdb
SELECT trades.sym, trades.qty, quotes.price FROM trades ASOF JOIN quotes ON trades.sym = quotes.sym AND trades.ts >= quotes.ts;

-- probe: positional_join duckdb
SELECT * FROM t POSITIONAL JOIN u;

-- probe: semi_join duckdb
SELECT * FROM t SEMI JOIN u USING (b);

-- probe: anti_join duckdb
SELECT * FROM t ANTI JOIN u USING (b);

-- probe: summarize duckdb
SUMMARIZE t;

-- probe: describe_query duckdb
DESCRIBE SELECT a, b FROM t;

-- probe: range_table_fn duckdb
SELECT * FROM range(3);

-- probe: generate_series duckdb
SELECT * FROM generate_series(1, 3);

-- probe: strftime_strptime duckdb
SELECT strftime(d, '%Y-%m') AS s, strptime('2026-01-02', '%Y-%m-%d') AS p FROM t;

-- probe: date_diff_part duckdb
SELECT date_diff('day', DATE '2026-01-01', d) AS dd, date_part('month', d) AS m FROM t;

-- probe: arg_max duckdb
SELECT arg_max(name, salary) AS n FROM emp;

-- probe: list_agg_fn duckdb
SELECT dept, list(name) AS l FROM emp GROUP BY dept;

-- probe: string_agg duckdb
SELECT dept, string_agg(name, ',') AS s FROM emp GROUP BY dept;

-- probe: quantile_cont duckdb
SELECT quantile_cont(salary, 0.5) AS q FROM emp;

-- probe: integer_div_op duckdb
SELECT 7 // 2 AS v;

-- probe: regexp_matches duckdb
SELECT regexp_matches(b, '^x') AS m FROM t;

-- probe: string_split duckdb
SELECT string_split('a,b', ',') AS s;

-- probe: json_arrow duckdb
SELECT '{"k": 1}'::JSON ->> 'k' AS v;

-- probe: union_by_name duckdb
SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a;

-- probe: using_sample duckdb
SELECT * FROM t USING SAMPLE 3 ROWS;

-- probe: limit_percent duckdb
SELECT * FROM t LIMIT 50%;

-- probe: trailing_comma duckdb
SELECT a, b, FROM t;

-- probe: copy_to duckdb
COPY (SELECT * FROM t) TO '{DATA}/out_copy.parquet' (FORMAT parquet);

-- probe: create_macro duckdb
CREATE MACRO add1(x) AS x + 1;

-- probe: where_select_alias duckdb
SELECT a * 2 AS x FROM t WHERE x > 4;

-- ---- Spark SQL idioms.

-- probe: backticks spark
SELECT `a` FROM t;

-- probe: lateral_view_explode spark
SELECT id, x FROM nested LATERAL VIEW explode(arr) e AS x;

-- probe: explode_select spark
SELECT id, explode(arr) AS x FROM nested;

-- probe: posexplode spark
SELECT posexplode(arr) FROM nested;

-- probe: named_struct spark
SELECT named_struct('p', 1, 'q', 'a') AS s;

-- probe: array_fn spark
SELECT array(1, 2, 3) AS l;

-- probe: map_fn spark
SELECT map('k', 1)['k'] AS v;

-- probe: hof_transform spark
SELECT transform(arr, x -> x + 1) AS l FROM nested;

-- probe: hof_filter spark
SELECT filter(arr, x -> x > 1) AS l FROM nested;

-- probe: hof_aggregate spark
SELECT aggregate(arr, CAST(0 AS BIGINT), (acc, x) -> acc + x) AS s FROM nested;

-- probe: hof_exists spark
SELECT exists(arr, x -> x > 2) AS e FROM nested;

-- probe: array_fns_spark spark
SELECT size(arr) AS n, array_contains(arr, 1) AS c, element_at(arr, 1) AS e, sort_array(arr) AS s, array_join(tags, ',') AS j FROM nested WHERE id = 1;

-- probe: sequence_fn spark
SELECT sequence(1, 3) AS s;

-- probe: collect_list_set spark
SELECT dept, collect_list(name) AS l, collect_set(year) AS s FROM emp GROUP BY dept;

-- probe: if_nvl spark
SELECT if(a > 2, 'hi', 'lo') AS i, nvl(a, 0) AS n, nvl2(a, 1, 0) AS n2 FROM t;

-- probe: date_fns_spark spark
SELECT date_add(d, 1) AS a, date_sub(d, 1) AS s, datediff(d, DATE '2026-01-01') AS dd, add_months(d, 1) AS am, last_day(d) AS ld FROM t;

-- probe: to_date_pattern spark
SELECT to_date('02/01/2026', 'dd/MM/yyyy') AS d;

-- probe: date_format spark
SELECT date_format(d, 'yyyy-MM') AS m FROM t;

-- probe: unix_time spark
SELECT from_unixtime(0) AS f, unix_timestamp(ts) AS u FROM t;

-- probe: year_month_fns spark
SELECT year(d) AS y, month(d) AS m, dayofweek(d) AS dw FROM t;

-- probe: string_fns_spark spark
SELECT concat_ws('-', b, b) AS c, instr(b, 'x') AS i, split(name, 'a') AS s, initcap(name) AS ic, locate('a', name) AS lo FROM emp JOIN t ON true LIMIT 3;

-- probe: rlike spark
SELECT * FROM t WHERE b RLIKE '^x';

-- probe: null_safe_eq spark
SELECT a <=> NULL AS v FROM t;

-- probe: div_operator spark
SELECT 7 DIV 2 AS v;

-- probe: percentile_approx spark
SELECT percentile_approx(salary, 0.5) AS p, percentile(salary, 0.5) AS q FROM emp;

-- probe: stack_fn spark
SELECT stack(2, 'a', 1, 'b', 2);

-- probe: left_semi_join spark
SELECT * FROM t LEFT SEMI JOIN u ON t.b = u.b;

-- probe: left_anti_join spark
SELECT * FROM t LEFT ANTI JOIN u ON t.b = u.b;

-- probe: distribute_sort_by spark
SELECT * FROM t DISTRIBUTE BY b SORT BY a;

-- probe: cast_string_type spark
SELECT CAST(a AS STRING) AS s, CAST(a AS BIGINT) AS i FROM t;

-- probe: interval_quoted spark
SELECT d + INTERVAL '1' DAY AS d1 FROM t;

-- probe: pipe_syntax spark
FROM t |> WHERE a > 2 |> SELECT a;

-- probe: select_except spark
SELECT * EXCEPT (c) FROM t;

-- probe: identifier_clause spark
SELECT * FROM IDENTIFIER('t');

-- probe: tablesample_percent spark
SELECT * FROM t TABLESAMPLE (50 PERCENT);

-- probe: first_last_agg spark
SELECT dept, first(name) AS f, last(name) AS l FROM emp GROUP BY dept;

-- probe: show_columns spark
SHOW COLUMNS IN t;

-- probe: describe_table_kw spark
DESCRIBE TABLE t;

-- probe: regexp_like spark
SELECT regexp_like(b, '^x') AS m FROM t;

-- ---- Semantics: what value does the statement give?

-- probe: int_division semantics
SELECT 7 / 2 AS v;

-- probe: nulls_order_asc semantics
SELECT a AS v FROM t ORDER BY a LIMIT 1;

-- probe: nulls_order_desc semantics
SELECT a AS v FROM t ORDER BY a DESC LIMIT 1;

-- probe: concat_with_null semantics
SELECT CONCAT('a', NULL) AS v;

-- probe: pipe_concat_null semantics
SELECT 'a' || NULL AS v;

-- probe: int_div_by_zero semantics
SELECT 1 / 0 AS v;

-- probe: float_div_by_zero semantics
SELECT CAST(1 AS DOUBLE) / 0 AS v;

-- probe: bigint_overflow semantics
SELECT CAST(9223372036854775807 AS BIGINT) + 1 AS v;

-- probe: int32_literal_overflow semantics
SELECT 2147483647 + 1 AS v;

-- probe: bad_cast semantics
SELECT CAST('abc' AS INTEGER) AS v;

-- probe: column_case semantics
SELECT A AS v FROM t LIMIT 1;

-- probe: table_case semantics
SELECT a AS v FROM T LIMIT 1;

-- probe: round_half_pos semantics
SELECT ROUND(2.5) AS v;

-- probe: round_half_neg semantics
SELECT ROUND(-2.5) AS v;

-- probe: round_decimal_literal semantics
SELECT ROUND(0.125, 2) AS v;

-- probe: decimal_literal_eq semantics
SELECT 0.1 + 0.2 = 0.3 AS v;

-- probe: string_order_binary semantics
SELECT 'a' < 'B' AS v;

-- probe: implicit_string_vs_int semantics
SELECT '10' > 9 AS v;

-- probe: date_minus_date semantics
SELECT DATE '2026-03-01' - DATE '2026-02-01' AS v;

-- probe: extract_dow_sunday semantics
SELECT EXTRACT(DOW FROM DATE '2026-11-08') AS v;

-- probe: array_index_1 semantics
SELECT arr[1] AS v FROM nested WHERE id = 1;

-- probe: substring_pos_0 semantics
SELECT SUBSTRING('hello', 0, 2) AS v;

-- probe: like_case semantics
SELECT 'ABC' LIKE 'a%' AS v;

-- probe: mod_negative semantics
SELECT -7 % 3 AS v;

-- probe: union_int_string semantics
SELECT MIN(v) AS v FROM (SELECT 1 AS v UNION ALL SELECT 'a') AS s;

-- probe: where_int_as_bool semantics
SELECT COUNT(*) AS v FROM t WHERE 1;

-- probe: nan_max semantics
SELECT MAX(x) AS v FROM (VALUES (1.0), (CAST('NaN' AS DOUBLE))) AS s(x);

-- probe: avg_int_type semantics
SELECT AVG(a) AS v FROM t;

-- probe: count_star_vs_col semantics
SELECT COUNT(b) AS v FROM t;

-- probe: empty_string_vs_null semantics
SELECT COUNT(*) AS v FROM t WHERE b = '';
