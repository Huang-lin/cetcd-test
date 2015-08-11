#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include "../../cetcd/cetcd.h"
#include "../ctest/ctest.h"


int watcher_cb (void *userdata, cetcd_response *resp) {
	cetcd_response_print(resp);
	return 1;
}

void NODE_CMP(cetcd_response_node *node1,cetcd_response_node *node2,int ttl,int mIndex,int cIndex,int key,int value,int dir)
{
	if (ttl) ASSERT_EQUAL(node1->ttl, node2->ttl);
	if (mIndex) ASSERT_EQUAL(node1->modified_index, node2->modified_index);
	if (cIndex) ASSERT_EQUAL(node1->created_index, node2->created_index);
	if (key) ASSERT_STR(node1->key, node2->key);
	if (value) ASSERT_STR(node1->value, node2->value);
	if (dir) ASSERT_EQUAL(node1->dir, node2->dir);
}

typedef struct updata_par_t
{
	cetcd_client *cli;
	uint64_t utime;
	cetcd_string key;
	cetcd_string value;
	uint64_t ttl;
} updata_par;

typedef struct watch_par_t
{
	cetcd_client *cli;
	uint64_t utime;
	cetcd_string key;
	uint64_t index;
} watch_par;

cetcd_response *resp3,*resp4;
void *updata(void *arg)
{
	printf("updata start\n");
	updata_par *par = (updata_par*)arg;
	usleep(par->utime);
	resp3 = cetcd_set(par->cli, par->key, par->value, par->ttl);
	cetcd_response_print(resp3);
	printf("updata end\n");
}

void *watch(void *arg)
{
	printf("watch start\n");
	watch_par *par = (watch_par*)arg;
	usleep(par->utime);
	resp4 = cetcd_watch(par->cli, par->key, par->index);
	printf("watch end\n");
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

CTEST_DATA(SETandGET)
{
	cetcd_string key;
};

CTEST_SETUP(SETandGET)
{
	data->key = "/aNewKey";
}

CTEST_TEARDOWN(SETandGET)
{
	cetcd_response *resp;
	resp = cetcd_delete(&cli, data->key);
	cetcd_response_release(resp);
}

CTEST2(SETandGET,NORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;

	cetcd_string value = "12323";
	uint64_t ttl = 0;
	resp = cetcd_set(&cli, data->key, value, ttl);
	resp2 = cetcd_get(&cli, data->key );
	
	NODE_CMP(resp->node, resp2->node, 1, 1, 1, 1, 1, 0);

	//cetcd_response_print(resp);

	cetcd_response_release(resp);
	cetcd_response_release(resp2);
}

CTEST(GET,NOTFOUND)
{
	cetcd_response *resp;
	cetcd_string key = "/nothingToGet";
	resp = cetcd_get(&cli, key);
	ASSERT_NOT_NULL(resp->err);
	ASSERT_EQUAL(resp->err->ecode, 100);

	//cetcd_response_print(resp);
	cetcd_response_release(resp);
}

CTEST_SKIP(GET_RECURSIVE,NORMAL)
{
	cetcd_response *resp;
	cetcd_response *subKey;
	cetcd_response_node *subNode;
	int i;
	cetcd_string key = "/queue";
	cetcd_string subkey[3] = {"/queue/4151","/queue/4186","/queue/4187"};
	int sorted = 1;
	resp = cetcd_get_recursive(&cli, key, sorted);
    ASSERT_NOT_NULL(resp);
    for (i=0; i<3; ++i)	
	{
		subKey = cetcd_get(&cli, subkey[i]);
        ASSERT_NOT_NULL(subKey);
		subNode = cetcd_array_get(resp->node->nodes, i);
        ASSERT_NOT_NULL(subNode);
		NODE_CMP(subKey->node, subNode, 1, 1, 1, 1, 1, 1);
	}
	cetcd_response_release(subKey);
	cetcd_response_release(resp);
}

CTEST(UPDATE,NORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	uint64_t ttl = 0;
	cetcd_string key = "/somethingToUpdate";
	cetcd_string value1 = "level 1";
	cetcd_string value2 = "level 2";
	resp = cetcd_set(&cli, key, value1, ttl);
	resp2 = cetcd_set(&cli, key, value2, ttl);

	NODE_CMP(resp->node, resp2->prev_node, 1, 1, 1, 1, 1, 1);
	NODE_CMP(resp->node, resp2->node, 1, 0, 0, 1, 0, 1);
	ASSERT_EQUAL(resp->node->modified_index+1,resp2->node->modified_index);
	ASSERT_EQUAL(resp->node->created_index+1,resp2->node->created_index);
	ASSERT_STR(resp2->node->value, value2);
    cetcd_response_release(resp2);
    cetcd_response_release(resp);
}

CTEST(DELETE,NORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string key = "/somethingToDelete";
	cetcd_string value = "donotDeleteMePlease";
	uint64_t ttl = 0;
	resp = cetcd_set(&cli, key, value, ttl);
	resp2 = cetcd_delete(&cli, key);

	NODE_CMP(resp->node, resp2->prev_node, 0, 1, 1, 1, 1, 1);
	NODE_CMP(resp->node, resp2->node, 0, 0, 1, 1, 0, 0); 
	ASSERT_NULL(resp2->node->value);
	cetcd_response_release(resp2);

	resp2 = cetcd_get(&cli,key);
	ASSERT_NOT_NULL(resp2->err);
	ASSERT_EQUAL(resp2->err->ecode,100);

	//cetcd_response_print(resp);
	cetcd_response_release(resp);
	cetcd_response_release(resp2);
}

CTEST(DELETE,NOTFOUND)
{
	cetcd_response *resp;
	cetcd_string key = "/nothingToDelete";
	resp = cetcd_delete(&cli, key);
	ASSERT_NOT_NULL(resp->err);
	ASSERT_EQUAL(resp->err->ecode, 100);
	cetcd_response_release(resp);
}

CTEST_SKIP(MKDIR,NORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string key = "/aNewDir";
	uint64_t ttl = 0;
	resp = cetcd_mkdir(&cli, key, ttl);
	resp2 = cetcd_get(&cli, key);
	NODE_CMP(resp->node, resp2->node, 1, 1, 1, 1, 1, 1);
	ASSERT_EQUAL(resp->node->dir, 1);

	system("curl 'http://127.0.0.1:2379/v2/keys/aNewDir?dir=true' -XDELETE ");
	//cetcd_response_print(resp);
	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST(MKDIR,dirIsEXIST)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string key = "/aExistDir";
	uint64_t ttl = 0;
	resp2 = cetcd_get(&cli, key);
	if (resp2->node == NULL) resp2 = cetcd_mkdir(&cli, key, ttl);
	ASSERT_NOT_NULL(resp2->node);
	resp = cetcd_mkdir(&cli, key, ttl);
	ASSERT_NOT_NULL(resp->err);
	ASSERT_EQUAL(resp->err->ecode, 105);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST_SKIP(SETDIR,fileIsExist)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string key = "/keyToDir";
	cetcd_string value = "evolution";
	uint64_t ttl = 0 ;

	resp2 = cetcd_set(&cli, key, value, ttl);
	ASSERT_NOT_NULL(resp2->node);
	resp = cetcd_setdir(&cli, key, ttl);
    ASSERT_NOT_NULL(resp->node);
	ASSERT_EQUAL(resp->node->dir, 1);
	system("curl 'http://127.0.0.1:2379/v2/keys/keyToDir?dir=true' -XDELETE ");
	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST_SKIP(SETDIR,dirIsnotExist)
{
	cetcd_response *resp;
	cetcd_string key = "/aNewNewDir";
	uint64_t ttl = 0;

	resp = cetcd_setdir(&cli, key, ttl);
	ASSERT_NOT_NULL(resp->node);
	ASSERT_STR(resp->node->key, key);
	ASSERT_EQUAL(resp->node->dir, 1);
	system("curl 'http://127.0.0.1:2379/v2/keys/aNewNewDir?dir=true' -XDELETE ");

	cetcd_response_release(resp);
}
	
CTEST(SETDIR,dirIsExist)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string key = "/aExistDir";
	uint64_t ttl = 0;
	resp2 = cetcd_get(&cli, key);
	if (resp2->node == NULL) resp2 = cetcd_mkdir(&cli, key, ttl);
	ASSERT_NOT_NULL(resp2->node);
	resp = cetcd_setdir(&cli, key, ttl);
    ASSERT_NOT_NULL(resp->err);
	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST_DATA(CAS)
{
	cetcd_string key;
	cetcd_string value;
};

CTEST_SETUP(CAS)
{
	cetcd_response *resp;
	uint64_t ttl = 0;
	data->key = "/aKeyToCas";
	data->value = "one";
	resp = cetcd_set(&cli, data->key, data->value, ttl);
	cetcd_response_release(resp);
}

CTEST_TEARDOWN(CAS)
{
	cetcd_response *resp;
	resp = cetcd_delete(&cli, data->key);
	cetcd_response_release(resp);
}
	
CTEST2(CAS,byValueNORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string newValue = "two";
	uint64_t ttl = 0;
	resp2 = cetcd_get(&cli, data->key);
	resp = cetcd_cmp_and_swap(&cli, data->key, newValue, data->value, ttl);
	NODE_CMP(resp2->node, resp->prev_node, 1, 1, 1, 1, 1, 1);
	ASSERT_STR(resp->node->value, newValue);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST2(CAS,byValueNOTFOUND)
{
	cetcd_response *resp;
	cetcd_string prevValue = "two";
	cetcd_string newValue = "three";
	uint64_t ttl = 0;
	resp = cetcd_cmp_and_swap(&cli, data->key, newValue, prevValue, ttl);
	ASSERT_NOT_NULL(resp->err);
	ASSERT_EQUAL(resp->err->ecode, 101);
	ASSERT_STR(resp->err->message, "Compare failed");

	cetcd_response_release(resp);
}

CTEST2(CAS,byIndexNORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string newValue = "two";
	resp2 = cetcd_get(&cli, data->key);
	uint64_t ttl = 0;
	resp = cetcd_cmp_and_swap_by_index(&cli, data->key, newValue, resp2->node->modified_index, ttl);
	NODE_CMP(resp2->node, resp->prev_node, 1, 1, 1, 1, 1, 1);
	ASSERT_STR(resp->node->value, newValue);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}
	
CTEST2(CAS,byIndexNOTFOUND)
{
	cetcd_response *resp;
	cetcd_string newValue = "two";
	uint64_t ttl = 0;
	resp = cetcd_cmp_and_swap_by_index(&cli, data->key, newValue, 24, ttl);
	ASSERT_NOT_NULL(resp->err);
	ASSERT_EQUAL(resp->err->ecode, 101);
	ASSERT_STR(resp->err->message, "Compare failed");

	cetcd_response_release(resp);
}

CTEST_DATA(CAD)
{
	cetcd_string key; 
	cetcd_string value; 
};

CTEST_SETUP(CAD)
{
	cetcd_response *resp;
	data->key = "/aKeyToCad";
	data->value = "donotDeleteMePlease";
	uint64_t ttl = 0;
	resp = cetcd_set(&cli, data->key, data->value, ttl);
	cetcd_response_release(resp);
}

CTEST_TEARDOWN(CAD)
{
	cetcd_response *resp;
	resp = cetcd_delete(&cli, data->key);
	cetcd_response_release(resp);
}

CTEST2(CAD,byValueNORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	resp2 = cetcd_get(&cli, data->key);
	resp = cetcd_cmp_and_delete(&cli, data->key, data->value);
	NODE_CMP(resp2->node, resp->prev_node, 1, 1, 1, 1, 1, 1);
	ASSERT_NULL(resp->node->value);
	cetcd_response_release(resp2);

	resp2 = cetcd_get(&cli, data->key);
	ASSERT_NOT_NULL(resp2->err);
	ASSERT_EQUAL(resp2->err->ecode, 100);
	//cetcd_response_print(resp2);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST2(CAD,byValueNOTFOUND)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string prevValue = "ok";
	resp = cetcd_cmp_and_delete(&cli, data->key, prevValue);
	ASSERT_NOT_NULL(resp->err);
	ASSERT_EQUAL(resp->err->ecode, 101);

	resp2 = cetcd_get(&cli, data->key);
	ASSERT_NULL(resp2->err);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}


CTEST2(CAD,byIndexNORMAL)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	resp2 = cetcd_get(&cli, data->key);
	resp = cetcd_cmp_and_delete_by_index(&cli, data->key, resp2->node->modified_index);
	NODE_CMP(resp2->node, resp->prev_node, 1, 1, 1, 1, 1, 1);
	ASSERT_NULL(resp->node->value);
	cetcd_response_release(resp2);

	resp2 = cetcd_get(&cli, data->key);
	ASSERT_NOT_NULL(resp2->err);
	ASSERT_EQUAL(resp2->err->ecode, 100);
	//cetcd_response_print(resp2);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST2(CAD,byIndexNOTFOUND)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	resp = cetcd_cmp_and_delete_by_index(&cli, data->key, 24);
	ASSERT_NOT_NULL(resp->err);
	ASSERT_EQUAL(resp->err->ecode, 101);

	resp2 = cetcd_get(&cli, data->key);
	ASSERT_NULL(resp2->err);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}

CTEST_DATA(WATCH)
{
	cetcd_string key;
	cetcd_string value;
};

CTEST_SETUP(WATCH)
{
	data->key = "/prisoner";
	data->value = "inPrison";
	uint64_t ttl = 0;
	cetcd_response *resp;
	resp = cetcd_set(&cli, data->key, data->value, ttl);
	cetcd_response_release(resp);
}


CTEST_TEARDOWN(WATCH)
{
	cetcd_response *resp;
	resp = cetcd_delete(&cli, data->key);
	cetcd_response_release(resp);
}



CTEST2(WATCH,FORMER)
{
	cetcd_response *resp;
	cetcd_response *resp2;
	cetcd_string newValue = "escaped";
	uint64_t watch_index;
	uint64_t ttl = 0;

	resp2 = cetcd_get(&cli, data->key);
	ASSERT_NOT_NULL(resp2->node);
	watch_index = resp2->node->modified_index+1;
	cetcd_response_release(resp2);
	resp2 = cetcd_set(&cli, data->key, newValue, ttl);
	ASSERT_NOT_NULL(resp2->node);
    resp = cetcd_watch(&cli, data->key, watch_index);
	NODE_CMP(resp->node, resp2->node ,1 ,1 ,1 ,1 ,1 ,1);
	NODE_CMP(resp->prev_node, resp2->prev_node, 1, 1, 1, 1, 1, 1);

	cetcd_response_release(resp2);
	cetcd_response_release(resp);
}


CTEST2_SKIP(WATCH,FUTURE)
{
	cetcd_response *resp;
	cetcd_string key = data->key;
	resp = cetcd_get(&cli, key);
	cetcd_response_print(resp);
	watch_par wp;
	wp.key = data->key;
	wp.index = resp->node->created_index+1;
	wp.utime = 0;
	wp.cli = &cli;
	pthread_t watch_id, updata_id;
	pthread_create(&watch_id, NULL, &watch, &wp);

	updata_par up;
	up.utime = 200;
	up.cli = &cli;
	up.key = data->key;
	up.value = "escaped";
	up.ttl = 0;
	pthread_create(&updata_id, NULL, &updata, &up);
	pthread_join(updata_id, NULL);
	pthread_join(watch_id, NULL);
	cetcd_response_print(resp3);
	cetcd_response_release(resp);
}

 
CTEST(TEST,END)
{
    cetcd_array_destory(&addrs);
    cetcd_client_destroy(&cli);
} 
