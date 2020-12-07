# go-mydumper

## 思路
第一个想法是直接管道一下，然后上传s3：
```
mydumper -h xxx -p xxx| mc pipe s3/backup/xxx
```
然而发现mydumper只能写到磁盘，而且是开了多个线程同时去写到不同文件，所以没办法直接管道stdout。

继而尝试给 [maxbube/mydumper](github.com/maxbube/mydumper/) 加直接上传minio的接口，发现minio并没有C/C++的SDK啊。。。 
于是转而修改go版本mydumper代码。添加了支持minio存储的部分 [pkg/storage](./pkg/storage)。


## 构建

```
$git clone https://github.com/harryge00/go-mydumper
$cd go-mydumper
$make build
$./bin/mydumper   -h
$./bin/myloader   -h
```
构建镜像:
```
export GOOS=linux 
export GOARCH=amd64 
make build 
docker build -t hyge/mydumper .
```

## 配置说明
使用minio存储的配置部分如下：
```
[storage]
storagetype = minio
endpoint = play.minio.io:9000
accesskey = Q3AM3UQ867SPQQA43P2F
secretkey = zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
bucket = test-backup
useSSL = true
```
| 配置参数        | 说明           | 类型 |
| ------------- |:-------------:| -----:|
| storagetype      | 存储类型, `local` 或者`minio`. 默认为 `local`| 字符串 |
| endpoint      | minio地址 | 字符串 |
| accesskey      | minio 登录用户名| 字符串 |
| secretkey      | minio 登录密码| 字符串 |
| useSSL      | 是否使用SSL| 布尔 |


## 运行
1. 二进制运行
```
./bin/go-mydumper -c conf/mydumper.ini.sample
```
2. docker 运行
```
docker run -d -v $PWD/conf/mydumper.ini.sample:/mydumper.ini hyge/mydumper go-mydumper -c  /mydumper.ini
```
3. K8s 运行
需要先部署mysql, 修改configmap中mysql地址
```
kubectl apply -f example/configmap.yaml
kubectl apply -f example/backup-job.yaml
```

### mydumper

```
./bin/mydumper -h
Usage: ./bin/mydumper -c conf/mydumper.ini.sample
  -c string
    	config file

Examples:
$./bin/mydumper -c conf/mydumper.ini.sample
 2017/10/25 13:12:52.933391 dumper.go:35:         [INFO]        dumping.database[sbtest].schema...
 2017/10/25 13:12:52.937743 dumper.go:45:         [INFO]        dumping.table[sbtest.benchyou0].schema...
 2017/10/25 13:12:52.937791 dumper.go:168:        [INFO]        dumping.table[sbtest.benchyou0].datas.thread[1]...
 2017/10/25 13:12:52.939008 dumper.go:45:         [INFO]        dumping.table[sbtest.benchyou1].schema...
 2017/10/25 13:12:52.939055 dumper.go:168:        [INFO]        dumping.table[sbtest.benchyou1].datas.thread[2]...
 2017/10/25 13:12:55.611905 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[633987].bytes[128MB].part[1].thread[1]
 2017/10/25 13:12:55.765127 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[633987].bytes[128MB].part[1].thread[2]
 2017/10/25 13:12:58.146093 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[1266050].bytes[256MB].part[2].thread[1]
 2017/10/25 13:12:58.253219 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[1266054].bytes[256MB].part[2].thread[2]

 ...
 [stripped]
 ...

 2017/10/25 13:13:02.939278 dumper.go:182:        [INFO]        dumping.allbytes[1024MB].allrows[5054337].time[10.01sec].rates[102.34MB/sec]...
 2017/10/25 13:13:35.496439 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[11345659].bytes[2304MB].part[18].thread[2]
 2017/10/25 13:13:37.627178 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou0].rows[11974624].bytes[2432MB].part[19].thread[1]
 2017/10/25 13:13:37.753966 dumper.go:105:        [INFO]        dumping.table[sbtest.benchyou1].rows[11974630].bytes[2432MB].part[19].thread[2]
 2017/10/25 13:13:39.453430 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou0].done.allrows[12486842].allbytes[2536MB].thread[1]...
 2017/10/25 13:13:39.453462 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou0].datas.thread[1].done...
 2017/10/25 13:13:39.622390 dumper.go:122:        [INFO]        dumping.table[sbtest.benchyou1].done.allrows[12484135].allbytes[2535MB].thread[2]...
 2017/10/25 13:13:39.622423 dumper.go:170:        [INFO]        dumping.table[sbtest.benchyou1].datas.thread[2].done...
 2017/10/25 13:13:39.622454 dumper.go:188:        [INFO]        dumping.all.done.cost[46.69sec].allrows[24970977].allbytes[5318557708].rate[108.63MB/s]
```

The dump files:
```
$ ls sbtest.sql/
metadata                    sbtest.benchyou0.00009.sql  sbtest.benchyou0.00018.sql   sbtest.benchyou1.00006.sql  sbtest.benchyou1.00015.sql
sbtest.benchyou0.00001.sql  sbtest.benchyou0.00010.sql  sbtest.benchyou0.00019.sql   sbtest.benchyou1.00007.sql  sbtest.benchyou1.00016.sql
sbtest.benchyou0.00002.sql  sbtest.benchyou0.00011.sql  sbtest.benchyou0.00020.sql   sbtest.benchyou1.00008.sql  sbtest.benchyou1.00017.sql
sbtest.benchyou0.00003.sql  sbtest.benchyou0.00012.sql  sbtest.benchyou0-schema.sql  sbtest.benchyou1.00009.sql  sbtest.benchyou1.00018.sql
sbtest.benchyou0.00004.sql  sbtest.benchyou0.00013.sql  sbtest.benchyou1.00001.sql   sbtest.benchyou1.00010.sql  sbtest.benchyou1.00019.sql
sbtest.benchyou0.00005.sql  sbtest.benchyou0.00014.sql  sbtest.benchyou1.00002.sql   sbtest.benchyou1.00011.sql  sbtest.benchyou1.00020.sql
sbtest.benchyou0.00006.sql  sbtest.benchyou0.00015.sql  sbtest.benchyou1.00003.sql   sbtest.benchyou1.00012.sql  sbtest.benchyou1-schema.sql
sbtest.benchyou0.00007.sql  sbtest.benchyou0.00016.sql  sbtest.benchyou1.00004.sql   sbtest.benchyou1.00013.sql  sbtest-schema-create.sql
sbtest.benchyou0.00008.sql  sbtest.benchyou0.00017.sql  sbtest.benchyou1.00005.sql   sbtest.benchyou1.00014.sql
```

### myloader

```
$ ./bin/myloader --help
Usage: ./bin/myloader -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -d  [DIR]
  -P int
    	TCP/IP port to connect to (default 3306)
  -d string
    	Directory of the dump to import
  -h string
    	The host to connect to
  -p string
    	User password
  -t int
    	Number of threads to use (default 16)
  -u string
    	Username with privileges to run the loader

Examples:
$./bin/myloader -h 192.168.0.2 -P 3306 -u mock -p mock -d sbtest.sql
 2017/10/25 13:04:17.396002 loader.go:75:         [INFO]        restoring.database[sbtest]
 2017/10/25 13:04:17.458076 loader.go:99:         [INFO]        restoring.schema[sbtest.benchyou0]
 2017/10/25 13:04:17.516236 loader.go:99:         [INFO]        restoring.schema[sbtest.benchyou1]
 2017/10/25 13:04:17.516389 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00015].thread[1]
 2017/10/25 13:04:17.516456 loader.go:115:        [INFO]        restoring.tables[benchyou0].parts[00005].thread[2]

...
[stripped]
...

 2017/10/25 13:05:27.783560 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00005].thread[9].done...
 2017/10/25 13:05:36.133758 loader.go:181:        [INFO]        restoring.allbytes[4087MB].time[78.62sec].rates[51.99MB/sec]...
 2017/10/25 13:05:44.759183 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00001].thread[3].done...
 2017/10/25 13:05:46.133728 loader.go:181:        [INFO]        restoring.allbytes[4216MB].time[88.62sec].rates[47.58MB/sec]...
 2017/10/25 13:05:46.567156 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00016].thread[6].done...
 2017/10/25 13:05:50.612200 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00008].thread[10].done...
 2017/10/25 13:05:51.131155 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00014].thread[2].done...
 2017/10/25 13:05:51.185629 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00011].thread[1].done...
 2017/10/25 13:05:51.836354 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00004].thread[0].done...
 2017/10/25 13:05:52.286931 loader.go:131:        [INFO]        restoring.tables[benchyou1].parts[00006].thread[11].done...
 2017/10/25 13:05:52.602444 loader.go:131:        [INFO]        restoring.tables[benchyou0].parts[00019].thread[8].done...
 2017/10/25 13:05:52.602573 loader.go:187:        [INFO]        restoring.all.done.cost[95.09sec].allbytes[5120.00MB].rate[53.85MB/s]
```

## License

go-mydumper is released under the GPLv3. See LICENSE
