# Keygen

[English](./README.md) | 简体中文

一个分布式主键生成器。

## 与雪花算法的区别

Keygen id 在其比特序列中包含了表索引，这样可以在没有分表键的情况下定位分表。
例如，使用 Keygen id，我们可以：

```sql
select * from orders where id = 76362673717182593
```

使用雪花算法，则需要：

```sql
select * from orders where id = 76362673717182593 and user_id = 100
```

第一种情况在增删改查代码中如此常用，所以我们做了这个改进。

## 基准测试

```shell
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkID-12    	  153584	      7870 ns/op
```

见 [基准测试](./id_test.go).

## 许可证

本项目使用 MIT 许可证。
