# Keygen

English | [简体中文](./README.zh-CN.md)

A Distributed Primary Key generator.

## Differences with the snowflake algorithm

Keygen id contains a table index in it's bit sequence, so we can locate the sharding table whitout the sharding key.
For example, use Keygen id, we could use:

```sql
select * from orders where id = 76362673717182593
```

Use snowflake, you should use:

```sql
select * from orders where id = 76362673717182593 and user_id = 100
```

The former is commonly used in CRUD codes so we made this improvement.

## Benchmarks

```shell
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkID-12    	  153584	      7870 ns/op
```

See [Benchmark tests](./id_test.go).

## License

This project under MIT license.
