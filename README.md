#cetcd_test
 
### 1.cetcd_test是用来干什么的

``` 
测试cetcd的各类API能否正确运行
```
### 2.怎么用
#### 2.1 依赖
```
cetcd的安装见cetcd目录中的README.md
ctest测试框架已附在tests目录中
```
#### 2.2 安装及运行
```
git clone https://github.com/Huang-lin/cetcd-test.git
cd cetcd_test/tests/mytest
make
./test
```
### 3)测试的功能
####括号外的为测试用例，括号内为预测的运行结果。
```
SET
    1、创建（成功）。2、覆盖（成功）。
UPDATE
    1、更新存在的key（成功）。2、更新不存在的key（失败）。
CREATE
     1、创建文件（成功）。2、创建已经存在的文件（失败）。
CREATE_IN_ORDER
     1、创建队列（成功）。
SETDIR
     1、覆盖存在的文件夹（失败）。2、覆盖存在的k-v对（成功）。
UPDATEDIR
     1、更新存在的文件夹（成功）。2、更新不存在的文件夹（失败）。
GET
     1、获取存在的文件（成功）。2、获取不存在的文件（失败）。
LSDIR
     1、有序地获取文件（成功）。2、递归地获取文件（成功）。
DELETE
     1、删除kv-pair（成功）。2、删除非空文件夹（成功）。3、删除不存在的key（失败）。
RMDIR
     1、删除空文件夹（成功）。2、删除非空文件夹（失败）。3、删除不存在的key（失败）。
CMP_AND_SWAP
     1、比较并修改文件（成功）。2、比较并修改，使用错误的prev_value（失败）。
CMP_AND_SWAP_BY_INDEX
     1、比较并修改文件（成功）。2、比较并修改，使用错误的prev_index（失败）。
CMP_AND_DELETE
     1、比较并删除文件（成功）。2、比较并删除，使用错误的prev_value（失败）。
CMP_AND_DELETE_BY_INDEX
     1、比较并删除文件（成功）。2、比较并删除，使用错误的prev_index（失败）。

```