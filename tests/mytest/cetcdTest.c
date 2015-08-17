#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include "../cetcd.h"
#include "../ctest/ctest.h"

typedef struct updata_par_t
{
	uint64_t utime;
	cetcd_string key;
	cetcd_string value;
	uint64_t ttl;
} updata_par;

void *updata(void *arg)
{
    cetcd_response *resp;
	updata_par *par = (updata_par*)arg;
	usleep(par->utime);

    cetcd_client cli1;
    cetcd_array addrs1;
	cetcd_array_init(&addrs1, 3);
	cetcd_array_append(&addrs1, "127.0.0.1:2379");
	cetcd_array_append(&addrs1, "127.0.0.1:2378");
	cetcd_array_append(&addrs1, "127.0.0.1:2377");
	cetcd_client_init(&cli1, &addrs1);

	resp = cetcd_set(&cli1, par->key, par->value, par->ttl);
    ASSERT_NOT_NULL(resp);
    cetcd_response_release(resp);

    cetcd_array_destory(&addrs1);
    cetcd_client_destroy(&cli1);
}

cetcd_client cli;
cetcd_array addrs;

CTEST(TEST,BEGIN)
{
	cetcd_array_init(&addrs, 3);
	cetcd_array_append(&addrs, "127.0.0.1:2379");
	cetcd_array_append(&addrs, "127.0.0.1:2378");
	cetcd_array_append(&addrs, "127.0.0.1:2377");

	cetcd_client_init(&cli, &addrs);
}

CTEST(set,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/aKey";
    cetcd_string value = "aValue";
    uint64_t ttl = 10;

    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, value);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    ASSERT_NULL(resp->prev_node);
    cetcd_response_release(resp);

    cetcd_string newValue = "aNewValue";
    resp = cetcd_set(&cli, key, newValue, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_STR(resp->prev_node->value, value);
    ASSERT_EQUAL(resp->prev_node->ttl, ttl);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, newValue);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(update,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/foo";
    cetcd_string value = "bar";
    uint64_t ttl = 10;
    
    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);

    cetcd_string newValue = "bar2";
    resp = cetcd_update(&cli, key, newValue, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, newValue);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_STR(resp->prev_node->value, value);
    ASSERT_EQUAL(resp->prev_node->ttl, ttl);
    cetcd_response_release(resp);

    cetcd_string noExistKey = "/noExistKey";
    resp = cetcd_update(&cli, noExistKey, value, ttl);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
    resp = cetcd_delete(&cli, noExistKey, 1);
    cetcd_response_release(resp);
}

CTEST(create,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/aNewKey";
    cetcd_string value = "aNewValue";
    uint64_t ttl = 10;

    resp = cetcd_create(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, value);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    cetcd_response_release(resp);

    resp = cetcd_create(&cli, key, value, ttl);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(create_in_order,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/queue";
    cetcd_string value1 = "value1";
    cetcd_string value2 = "value2";
    uint64_t ttl = 10;

    resp = cetcd_create_in_order(&cli, key, value1, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->value, value1);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    int firstKey = atoi(resp->node->key+strlen(key)+1);
    cetcd_response_release(resp);
    resp = cetcd_create_in_order(&cli, key, value2, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->value, value2);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    int secondKey = atoi(resp->node->key+strlen(key)+1);
    ASSERT_TRUE(firstKey < secondKey);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(setdir,TEST)
{
    cetcd_response *resp;
    cetcd_string dirKey = "/fooDir";
    uint64_t ttl = 10; 
    
    resp = cetcd_setdir(&cli, dirKey, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, dirKey);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    ASSERT_NULL(resp->prev_node);
    cetcd_response_release(resp);

    resp = cetcd_setdir(&cli, dirKey, ttl);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    cetcd_string key = "/foo";
    cetcd_string value = "aaaa";
    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    
    resp = cetcd_setdir(&cli, key, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_STR(resp->prev_node->value, value);
    ASSERT_EQUAL(resp->prev_node->ttl, ttl);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, dirKey, 1);
    cetcd_response_release(resp);
    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(updatedir,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/aDir";
    uint64_t ttl = 10; 

    resp = cetcd_mkdir(&cli, key, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_updatedir(&cli, key, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_NULL(resp->node->value);
    ASSERT_TRUE(resp->node->dir);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_TRUE(resp->prev_node->dir);
    ASSERT_EQUAL(resp->prev_node->ttl, ttl);
    cetcd_response_release(resp);

    cetcd_string noExistDir = "/noExistDir";
    resp = cetcd_updatedir(&cli, noExistDir, ttl);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(get,Test)
{
    cetcd_response *resp;
    cetcd_string key = "/aKey";
    cetcd_string value = "aValue";
    uint64_t ttl = 10;

    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_get(&cli, key);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, value);
    cetcd_response_release(resp);

    cetcd_string noExistKey = "/noExistKey";
    resp = cetcd_get(&cli, noExistKey);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(get_recursive,TEST)
{
    cetcd_response *resp;
    cetcd_string dirKey = "/fooDir";
    cetcd_string key0 = "/fooDir/k0";
    cetcd_string value0 = "v0";
    cetcd_string key1 = "/fooDir/k1";
    cetcd_string value1 = "v1";
    uint64_t ttl = 10;
    
    resp = cetcd_mkdir(&cli, dirKey, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_set(&cli, key0, value0, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_set(&cli, key1, value1, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_lsdir(&cli, dirKey, 1, 0);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, dirKey);
    ASSERT_TRUE(resp->node->dir);
    cetcd_response_node *subNode;
    subNode = cetcd_array_get(resp->node->nodes, 0);
    ASSERT_STR(subNode->key, key0);
    ASSERT_STR(subNode->value, value0);
    ASSERT_EQUAL(subNode->ttl, ttl);
    subNode = cetcd_array_get(resp->node->nodes, 1);
    ASSERT_STR(subNode->key, key1);
    ASSERT_STR(subNode->value, value1);
    ASSERT_EQUAL(subNode->ttl, ttl);
    cetcd_response_release(resp);

    cetcd_string key2 = "/fooDir/dir/key";
    cetcd_string value2 = "value2";
    resp = cetcd_set(&cli, key2, value2, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_lsdir(&cli, dirKey, 1, 1);
    subNode = cetcd_array_get(
            ((cetcd_response_node*)cetcd_array_get(resp->node->nodes, 0))->nodes, 0);
    ASSERT_STR(subNode->key, key2);
    ASSERT_STR(subNode->value, value2);
    ASSERT_EQUAL(subNode->ttl, ttl);
    subNode = cetcd_array_get(resp->node->nodes, 1);
    ASSERT_STR(subNode->key, key0);
    ASSERT_STR(subNode->value, value0);
    ASSERT_EQUAL(subNode->ttl, ttl);
    subNode = cetcd_array_get(resp->node->nodes, 2);
    ASSERT_STR(subNode->key, key1);
    ASSERT_STR(subNode->value, value1);
    ASSERT_EQUAL(subNode->ttl, ttl);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, dirKey, 1);
    cetcd_response_release(resp);
}

CTEST(delete,Test)
{
    cetcd_response *resp;
    cetcd_string key = "/somethingToDelete";
    cetcd_string value = "aaabbb";
    uint64_t ttl = 10;
    
    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_delete(&cli, key, 0);
    ASSERT_NULL(resp->err);
    ASSERT_NULL(resp->node->value);
    ASSERT_STR(resp->prev_node->value, value);
    cetcd_response_release(resp);

    resp = cetcd_mkdir(&cli, key, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    char *secondKey = "/somethingToDelete/aaa";
    resp = cetcd_set(&cli, secondKey, value, ttl);
    cetcd_response_release(resp);
    resp = cetcd_delete(&cli, key, 1);
    ASSERT_NULL(resp->err);
    ASSERT_NULL(resp->node->value);
    ASSERT_TRUE(resp->prev_node->dir);
    ASSERT_NULL(resp->prev_node->value);
    cetcd_response_release(resp);

    cetcd_string noExistKey = "/noExistKey";
    resp = cetcd_delete(&cli, noExistKey, 0);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(rmdir,Test)
{
    cetcd_response *resp;
    cetcd_string key = "/somethingToDelete";
    cetcd_string value = "aaabbb";
    uint64_t ttl = 10;
    
    resp = cetcd_mkdir(&cli, key, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_rmdir(&cli, key);
    ASSERT_NULL(resp->err);
    ASSERT_NULL(resp->node->value);
    ASSERT_NULL(resp->prev_node->value);
    ASSERT_TRUE(resp->prev_node->dir);
    cetcd_response_release(resp);

    resp = cetcd_mkdir(&cli, key, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    char *secondKey = (char*)malloc(40);
    strcpy(secondKey,key);
    strcat(secondKey,"/aaa");
    resp = cetcd_set(&cli, secondKey, value, ttl);
    free(secondKey);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_rmdir(&cli, key);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    cetcd_string noExistKey = "/noExistKey";
    resp = cetcd_rmdir(&cli, noExistKey);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(cmp_and_swap,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/foo";
    cetcd_string value = "bar";
    cetcd_string newValue = "bar2";
    uint64_t ttl = 10;

    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_cmp_and_swap(&cli, key, newValue, value, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, newValue);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_STR(resp->prev_node->value, value);
    ASSERT_EQUAL(resp->prev_node->ttl, ttl);
    cetcd_response_release(resp);

    cetcd_string noExistValue = "noExistValue";
    resp = cetcd_cmp_and_swap(&cli, key, newValue, noExistValue, ttl);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(cmp_and_swap_by_index,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/foo";
    cetcd_string value = "bar";
    cetcd_string newValue = "bar2";
    uint64_t ttl = 10;

    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    uint64_t index = resp->node->modified_index;
    cetcd_response_release(resp);
    resp = cetcd_cmp_and_swap_by_index(&cli, key, newValue, index, ttl);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, newValue);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_STR(resp->prev_node->value, value);
    ASSERT_EQUAL(resp->prev_node->ttl, ttl);
    cetcd_response_release(resp);

    uint64_t noExistIndex = 69786876876;
    resp = cetcd_cmp_and_swap_by_index(&cli, key, newValue, noExistIndex, ttl);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(cmp_and_delete,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/foo";
    cetcd_string value = "bar";
    uint64_t ttl = 10;

    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_cmp_and_delete(&cli, key, value);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_NULL(resp->node->value);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_STR(resp->prev_node->value, value);
    cetcd_response_release(resp);

    cetcd_string  noExistValue = "noExistValue";
    resp = cetcd_cmp_and_delete(&cli, key, noExistValue);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(cmp_and_delete_by_index,TEST)
{
    cetcd_response *resp;
    cetcd_string key = "/foo";
    cetcd_string value = "bar";
    uint64_t ttl = 10;

    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    uint64_t index = resp->node->modified_index;
    cetcd_response_release(resp);
    resp = cetcd_cmp_and_delete_by_index(&cli, key, index);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_NULL(resp->node->value);
    ASSERT_STR(resp->prev_node->key, key);
    ASSERT_STR(resp->prev_node->value, value);
    cetcd_response_release(resp);

    uint64_t noExistIndex = 587687687689769;
    resp = cetcd_cmp_and_delete_by_index(&cli, key, noExistIndex);
    ASSERT_NOT_NULL(resp->err);
    cetcd_response_release(resp);

    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(watch,TEST) 
{ 
	cetcd_response *resp;
    cetcd_string key = "/somethingToWatch";
    cetcd_string value = "aaa";
    cetcd_string newValue = "bbb";
    uint64_t ttl = 50;

    resp = cetcd_set(&cli, key, value, ttl);
    ASSERT_NULL(resp->err);
    uint64_t watchIndex = resp->node->modified_index+1;
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
	pthread_t updata_id;
	updata_par up;
	up.utime = 10000;
	up.key = key;
	up.value = newValue;
	up.ttl = ttl;
	pthread_create(&updata_id, NULL, &updata, &up);
    resp = cetcd_watch(&cli, key, watchIndex);
	pthread_join(updata_id, NULL);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, key);
    ASSERT_STR(resp->node->value, newValue);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    cetcd_response_release(resp);
    
    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}

CTEST(watch_recursive,TEST) 
{ 
	cetcd_response *resp;
    cetcd_string key = "/foo";
    cetcd_string subKey = "/foo/foo1";

    cetcd_string value = "aaa";
    cetcd_string newValue = "bbb";
    uint64_t ttl = 50;

    resp = cetcd_mkdir(&cli, key, ttl);
    ASSERT_NULL(resp->err);
    cetcd_response_release(resp);
    resp = cetcd_set(&cli, subKey, value, ttl);
    ASSERT_NULL(resp->err);
    uint64_t watchIndex = resp->node->modified_index+1;
    cetcd_response_release(resp);
	pthread_t updata_id;
	updata_par up;
	up.utime = 100000;
	up.key = subKey;
	up.value = newValue;
	up.ttl = ttl;
	pthread_create(&updata_id, NULL, &updata, &up);
    resp = cetcd_watch_recursive(&cli, key, watchIndex);
	pthread_join(updata_id, NULL);
    ASSERT_NULL(resp->err);
    ASSERT_STR(resp->node->key, subKey);
    ASSERT_STR(resp->node->value, newValue);
    ASSERT_EQUAL(resp->node->ttl, ttl);
    cetcd_response_release(resp);
    
    resp = cetcd_delete(&cli, key, 1);
    cetcd_response_release(resp);
}
CTEST(TEST,END)
{
    cetcd_array_destory(&addrs);
    cetcd_client_destroy(&cli);
} 
