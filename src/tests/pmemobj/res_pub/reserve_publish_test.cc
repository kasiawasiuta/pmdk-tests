/*
 * Copyright (c) 2018, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in
 * the documentation and/or other materials provided with the
 * distribution.
 *
 * * Neither the name of Intel Corporation nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY LOG OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "reserve_publish.h"
#include <thread>

/**
 * RESERVE_PUBLISH_PUBLISH
 * Parameterized Test Case: Checks that objects are correctly populated and
 * allocated. Parameters are:
 *  - the number of threads (n)
 *  - the size of the objects to be reserved
 *  - the number of objects to be reserved by each thread
 * \test
 *          \li \c Step1. Create the PMEMobj pool file
 *          / SUCCESS function returns pointer to pool
 *          \li \c Step2. Make reservations with pmemobj_reserve using n threads
 *          \li \c Step3. Populate the reserved objects within the thread.
 *          \li \c Step4. Publish the objects within the thread with
 *          pmemobj_publish / SUCCESS function returns 0
 *          \li \c Step5. Synchronize the threads
 *          \li \c Step6. Close and reopen the PMEMobj pool
 *          \li \c Step7. Verify that the objects were properly allocated
 *          \li \c Step8. Close the PMEMobj pool
 *          \li \c Step9. Remove the PMEMobj pool
 */
TEST_P(PMemObjReservePublishParamTest, RESERVE_PUBLISH_PUBLISH) {
  std::vector<std::thread> threads;
  size_t nof_messages = nof_threads * messages_per_thread;
  pobj_action* act = new pobj_action[nof_messages];

  /* Step 1 */
  pop = pmemobj_create(pool_path_.c_str(), LAYOUT_NAME, pool_size,
                       S_IWRITE | S_IREAD);
  ASSERT_TRUE(pop != nullptr) << pmemobj_errormsg();

  for (int th = 0; th < nof_threads; th++) {
    test_obj obj;
    obj.act = &act[th * messages_per_thread];
    threads.push_back(
        std::thread(&PMemObjReservePublishTest::ReservePublishInThread, this,
                    std::ref(obj)));
  }

  /* Step 5 */
  for (auto& th : threads) {
    th.join();
  }
  threads.clear();

  /* Step 6 */
  pmemobj_close(pop);
  pop = pmemobj_open(pool_path_.c_str(), LAYOUT_NAME);

  /* Step 7 */
  TOID(struct message) read_message = POBJ_FIRST(pop, struct message);
  for (int i = 0; i < nof_messages; i++) {
    ASSERT_FALSE(OID_IS_NULL(read_message.oid));
    for (int j = 0; j < data_size; j++) {
      ASSERT_TRUE(D_RO(read_message)->data[j] == pattern);
    }
    read_message = POBJ_NEXT(read_message);
  }
  ASSERT_TRUE(OID_IS_NULL(read_message.oid));

  /* Step 8 */
  pmemobj_close(pop);

  /* Step 9 */
  ApiC::RemoveFile(pool_path_);
  delete[] act;
}

/**
 * RESERVE_PUBLISH_CANCEL
 * Parameterized Test Case: Checks that objects are indeed not allocated if the
 * reservation is cancelled.
 *  - the number of threads (n)
 *  - the size of the objects to be reserved
 * \test
 *          \li \c Step1. Create the PMEMobj pool file
 *          / SUCCESS function returns pointer to pool
 *          \li \c Step2. Make reservations until the entire PMEMobj pool is
 *          filled
 *          \li \c Step3. Cancel all reserved objects
 *          \li \c Step4. Make reservations using n threads
 *          \li \c Step5. Verify that the same number of reservations were made
 *          in Step2 and Step4
 *          \li \c Step6 Cancel every 4th reserved object with pmomobj_cancel
 *          \li \c Step7. Make reservations until the entire PMEMobj pool is
 *          filled
 *          \li \c Step8. Verify that the number of reservations made in Step7
 *          equals the number of cancellations in Step6
 *          \li \c Step9. Close the PMEMobj pool
 *          \li \c Step10. Remove the PMEMobj pool
 */
TEST_P(PMemObjReservePublishParamTest, RESERVE_PUBLISH_CANCEL) {
  std::vector<std::thread> threads;
  size_t max_messages = pool_size / data_size;
  pobj_action* act = new pobj_action[max_messages];

  /* Step 1 */
  pop = pmemobj_create(pool_path_.c_str(), LAYOUT_NAME, pool_size,
                       S_IWRITE | S_IREAD);
  ASSERT_TRUE(pop != nullptr) << pmemobj_errormsg();

  /* Step 2 */
  int actual_nof_messages = makeMaximumAllocations(pop, data_size, &act[0]);

  /* Step 3 */
  pmemobj_cancel(pop, act, actual_nof_messages);

  messages_per_thread = actual_nof_messages / nof_threads;
  size_t deletes_per_thread =
      (messages_per_thread + index_to_delete - 1) / index_to_delete;
  size_t publications_per_thread = messages_per_thread - deletes_per_thread;
  size_t actual_deletes = nof_threads * deletes_per_thread;
  size_t actual_publications = nof_threads * publications_per_thread;
  pobj_action* act_delete = new pobj_action[actual_deletes];

  /* Step 4 */
  for (int th = 0; th < nof_threads; th++) {
    test_obj obj;
    obj.act = &act[th * (publications_per_thread)];
    obj.act_delete = &act_delete[th * deletes_per_thread];
    threads.push_back(
        std::thread(&PMemObjReservePublishTest::ReservePublishCancelInThread,
                    this, std::ref(obj)));
  }
  for (auto& th : threads) {
    th.join();
  }
  threads.clear();

  pmemobj_publish(pop, act, actual_publications);
  int remaining_space = makeMaximumAllocations(pop, data_size, &act[0]);

  /* Step 5 */
  ASSERT_EQ(actual_nof_messages,
            actual_publications + actual_deletes + remaining_space);

  /* Step 6 */
  pmemobj_cancel(pop, act_delete, actual_deletes);

  /* Step 7 */
  int free_space = makeMaximumAllocations(pop, data_size, &act[0]);

  /* Step 8 */
  ASSERT_EQ(free_space, actual_deletes);

  /* Step 9 */
  pmemobj_close(pop);

  /* Step 10 */
  ApiC::RemoveFile(pool_path_);
  delete[] act;
  delete[] act_delete;
}

/**
 * RESERVE_PUBLISH_DEFER_FREE_01
 * Parameterized Test Case: Verifies that objects are freed after publishing
 * defer_free actions
 *  - the number of threads (n)
 *  - the size of the objects to be reserved
 *  - the number of objects to be reserved by each thread
 * \test
 *          \li \c Step1. Create the PMEMobj pool file
 *          / SUCCESS function returns pointer to pool
 *          \li \c Step2. Make reservations with pmemobj_reserve using n threads
 *          \li \c Step3. Populate the reserved object in the worker thread
 *          \li \c Step4. Mark every 2nd object to be freed after publishing
 *          with pmemobj_defer_free
 *          \li \c Step5. Synchronize the threads
 *          \li \c Step6. Publish the reserved objects with pmemobj_publish
 *          / SUCCESS function returns 0
 *          \li \c Step7. Publish the free actions with pmemobj_publish
 *          \li \c Step8. Close and reopen the PMEMobj pool
 *          \li \c Step9. Verify that half of the OID's were freed.
 *          \li \c Step10. Close the PMEMobj pool with pmemobj_close
 *          \li \c Step11. Remove the PMEMobj pool
 */
TEST_P(PMemObjReservePublishParamTest, RESERVE_PUBLISH_DEFER_FREE_01) {
  size_t max_messages = pool_size / data_size;
  pobj_action* act = new pobj_action[max_messages];
  size_t total_messages = nof_threads * messages_per_thread;
  size_t deletes_per_thread = (messages_per_thread + 1) / 2;
  size_t total_deletes = nof_threads * deletes_per_thread;
  pobj_action* act_free = new pobj_action[total_deletes];

  std::vector<std::thread> threads;

  /* Step 1 */
  pop = pmemobj_create(pool_path_.c_str(), LAYOUT_NAME, pool_size,
                       S_IWRITE | S_IREAD);
  ASSERT_TRUE(pop != nullptr) << pmemobj_errormsg();

  for (int th = 0; th < nof_threads; th++) {
    test_obj obj;
    obj.act = &act[th * (messages_per_thread)];
    obj.act_delete = &act_free[th * deletes_per_thread];
    threads.push_back(
        std::thread(&PMemObjReservePublishTest::ReservePublishDeferFreeInThread,
                    this, std::ref(obj)));
  }

  /* Step 5 */
  for (auto& th : threads) {
    th.join();
  }
  threads.clear();

  int remaining_space =
      makeMaximumAllocations(pop, data_size, &act[total_messages]);

  /* Step 6 */
  int ret = pmemobj_publish(pop, act, total_messages + remaining_space);
  ASSERT_EQ(ret, 0);

  /* Step 7 */
  ret = pmemobj_publish(pop, act_free, total_deletes);
  ASSERT_EQ(ret, 0);

  /* Step 8 */
  pmemobj_close(pop);
  pop = pmemobj_open(pool_path_.c_str(), LAYOUT_NAME);

  /* Step 9 */
  int free_count = makeMaximumAllocations(pop, data_size, &act[0]);
  ASSERT_EQ(free_count, total_deletes);

  /* Step 10 */
  pmemobj_close(pop);

  /* Step 11 */
  ApiC::RemoveFile(pool_path_);
  delete[] act;
  delete[] act_free;
}

/**
 * RESERVE_PUBLISH_DEFER_FREE_02
 * Parameterized Test Case: Verifies that objects are not freed when the
 * defer_free actions are cancelled
 *  - the number of threads (n)
 *  - the size of the objects to be reserved
 *  - the number of objects to be reserved by each thread
 * \test
 *          \li \c Step1. Create the PMEMobj pool file
 *          / SUCCESS function returns pointer to pool
 *          \li \c Step2. Make reservations with pmemobj_reserve using n threads
 *          / SUCCESS function returns handle to newly created reserved object
 *          \li \c Step3. Populate the reserved object in the worker thread
 *          \li \c Step4. Mark every 2nd object to be freed after publishing
 *          with pmemobj_defer_free
 *          \li \c Step5. Synchronize the threads
 *          \li \c Step6. Publish the reserved objects with pmemobj_publish
 *          / SUCCESS function returns 0
 *          \li \c Step7. Cancel the free actions with pmemobj_cancel
 *          \li \c Step8. Close and reopen the PMEMobj pool
 *          \li \c Step9. Verify that none of the OID's were freed
 *          \li \c Step10. Close the PMEMobj pool with pmemobj_close
 *          \li \c Step11. Remove the PMEMobj pool
 */
TEST_P(PMemObjReservePublishParamTest, RESERVE_PUBLISH_DEFER_FREE_02) {
  size_t max_messages = pool_size / data_size;
  pobj_action* act = new pobj_action[max_messages];
  size_t total_messages = nof_threads * messages_per_thread;
  size_t deletes_per_thread = (messages_per_thread + 1) / 2;
  size_t total_deletes = nof_threads * deletes_per_thread;
  pobj_action* act_free = new pobj_action[total_deletes];

  std::vector<std::thread> threads;

  /* Step 1 */
  pop = pmemobj_create(pool_path_.c_str(), LAYOUT_NAME, pool_size,
                       S_IWRITE | S_IREAD);
  ASSERT_TRUE(pop != nullptr) << pmemobj_errormsg();

  for (int th = 0; th < nof_threads; th++) {
    test_obj obj;
    obj.act = &act[th * messages_per_thread];
    obj.act_delete = &act_free[th * deletes_per_thread];
    threads.push_back(
        std::thread(&PMemObjReservePublishTest::ReservePublishDeferFreeInThread,
                    this, std::ref(obj)));
  }

  /* Step 5 */
  for (auto& th : threads) {
    th.join();
  }
  threads.clear();

  int remaining_space =
      makeMaximumAllocations(pop, data_size, &act[total_messages]);

  /* Step 6 */
  int ret = pmemobj_publish(pop, act, total_messages + remaining_space);
  ASSERT_EQ(ret, 0);

  /* Step 7 */
  pmemobj_cancel(pop, act_free, total_deletes);

  /* Step 8 */
  pmemobj_close(pop);
  pop = pmemobj_open(pool_path_.c_str(), LAYOUT_NAME);

  /* Step 9 */
  int free_count = makeMaximumAllocations(pop, data_size, &act[0]);
  ASSERT_EQ(free_count, 0);

  /* Step 10 */
  pmemobj_close(pop);

  /* Step 11 */
  ApiC::RemoveFile(pool_path_);
  delete[] act;
  delete[] act_free;
}

/**
 * RESERVE_PUBLISH_ATOMIC
 * Positive Test Case: Checks that publications are fail-safe atomic.
 * \test
 *          \li \c Step1. Create the PMEMobj pool file
 *          / SUCCESS function returns pointer to pool
 *          \li \c Step2. Make reservations until the pool is completely filled.
 *          / SUCCESS function returns handles to created reserved object
 *          \li \c Step3. Populate the reserved objects
 *          \li \c Step4. Start publishing the reserved objects in a worker
 *          thread using pmemobj_publish
 *          \li \c Step5. Kill the worker thread after the first object is
 *          published using a cancellation point
 *          \li \c Step6. Make reservations until the pool is completely filled
 *          \li \c Step7. Verify that the number of reservations made in steps 2
 *          and 7 are equal to each other
 *          \li \c Step8. Close the PMEMobj pool
 *          \li \c Step9. Remove the PMEMobj pool
 */
TEST_F(PMemObjReservePublishTest, RESERVE_PUBLISH_ATOMIC) {
  size_t max_messages = pool_size / data_size;
  pobj_action* act = new pobj_action[max_messages];

  /* Step 1 */
  pop = pmemobj_create(pool_path_.c_str(), LAYOUT_NAME, pool_size,
                       S_IWRITE | S_IREAD);
  ASSERT_TRUE(pop != nullptr) << pmemobj_errormsg();

  /* Step 2 */
  /* Step 3 */
  messages_per_thread = makeMaximumAllocations(pop, data_size, &act[0]);
  messages_per_thread = 24;

  test_obj obj;
  obj.act = &act[0];

  /* Step 4 */
  std::thread worker = std::thread(
      &PMemObjReservePublishTest::ReservePublishWorker, this, std::ref(obj));
  worker.join();

  /* Step 5 */

  /* Step 6 */
  int count = makeMaximumAllocations(pop, data_size, &act[0]);

  /* Step 7 */
  ASSERT_EQ(messages_per_thread, count);

  /* Step 8 */
  pmemobj_close(pop);

  delete[] act;
}

/**
 * RESERVE_PUBLISH_XRESERVE_01
 * Parameterized Test Case: Checks the functionality of prmemobj_xreserve using
 * the POBJ_XALLOC_ZERO flag for the following parameters:
 *  - the number of threads (n)
 *  - the size of the objects to be reserved
 *  - the number of objects to be reserved by each thread
 * \test
 *          \li \c Step1. Create the PMEMobj pool file
 *          / SUCCESS function returns pointer to pool
 *          \li \c Step2. Make reservations with pmemobj_xreserve with the
 *          flags parameter set and using n threads
 *          \li \c Step3. Publish the reserved objects within the thread with
 *          pmemobj_xpublish / SUCCESS function returns 0
 *          \li \c Step4. Synchronize the threads
 *          \li \c Step5. Verify that the objects were properly allocated
 *          \li \c Step6. Close the PMEMobj pool
 *          \li \c Step7. Remove the PMEMobj pool
 */
TEST_P(PMemObjReservePublishParamTest, RESERVE_PUBLISH_XRESERVE_01) {
  size_t nof_messages = nof_threads * messages_per_thread;

  std::vector<std::thread> threads;
  pobj_action* act = new pobj_action[nof_threads * messages_per_thread];

  /* Step 1 */
  pop = pmemobj_create(pool_path_.c_str(), LAYOUT_NAME, pool_size,
                       S_IWRITE | S_IREAD);
  ASSERT_TRUE(pop != nullptr) << pmemobj_errormsg();

  for (int th = 0; th < nof_threads; th++) {
    test_x_obj obj;
    obj.flags = POBJ_XALLOC_ZERO;
    obj.act = &act[th * messages_per_thread];
    threads.push_back(std::thread(&PMemObjReservePublishTest::XReservePublish,
                                  this, std::ref(obj)));
  }

  /* Step 4 */
  for (auto& th : threads) {
    th.join();
  }
  threads.clear();

  /* Step 5 */
  ASSERT_EQ(get_nof_stored_messages(pop), nof_messages);

  /* Step 6 */
  pmemobj_close(pop);

  /* Step 7 */
  ApiC::RemoveFile(pool_path_);
  delete[] act;
}

/**
 * RESERVE_PUBLISH_XRESERVE_02
 * Parameterized Test Case: Checks the functionality of prmemobj_xreserve using
 * the POBJ_CLASS_ID flag for the following parameters:
 *  - the number of threads (n)
 *  - the size of the objects to be reserved
 *  - the number of objects to be reserved by each thread
 * \test
 *          \li \c Step1. Create the PMEMobj pool file
 *          / SUCCESS function returns pointer to pool
 *          \li \c Step2. Make reservations with pmemobj_xreserve with the
 *          flags parameter set and using n threads
 *          \li \c Step3. Publish the reserved objects within the thread with
 *          pmemobj_xpublish / SUCCESS function returns 0
 *          \li \c Step4. Synchronize the threads
 *          \li \c Step5. Verify that the objects were properly allocated
 *          \li \c Step6. Close the PMEMobj pool
 *          \li \c Step7. Remove the PMEMobj pool
 */
TEST_P(PMemObjReservePublishParamTest, RESERVE_PUBLISH_XRESERVE_02) {
  std::vector<std::thread> threads;

  size_t nof_messages = nof_threads * messages_per_thread;

  struct pobj_alloc_class_desc ac;
  ac.header_type = POBJ_HEADER_NONE;
  ac.unit_size = data_size;
  ac.units_per_block = 4;
  ac.alignment = 0;

  /* Step 1 */
  pop = pmemobj_create(pool_path_.c_str(), LAYOUT_NAME, 10 * PMEMOBJ_MIN_POOL,
                       S_IWRITE | S_IREAD);
  ASSERT_TRUE(pop != nullptr) << pmemobj_errormsg();

  int ret = pmemobj_ctl_set(pop, "heap.alloc_class.128.desc", &ac);
  EXPECT_EQ(errno, 0);
  ASSERT_EQ(ret, 0) << pmemobj_errormsg();

  for (int th = 0; th < nof_threads; th++) {
    test_x_obj obj;
    obj.flags = ac.class_id;
    threads.push_back(std::thread(&PMemObjReservePublishTest::XReservePublish,
                                  this, std::ref(obj)));
  }

  /* Step 4 */
  for (auto& th : threads) {
    th.join();
  }
  threads.clear();

  /* Step 5 */
  EXPECT_EQ(get_nof_stored_messages(pop), nof_messages);

  /* Step 6 */
  pmemobj_close(pop);

  /* Step 7 */
  ApiC::RemoveFile(pool_path_);
}

INSTANTIATE_TEST_CASE_P(
    ResPubParam, PMemObjReservePublishParamTest,
    ::testing::Values(
        reserve_publish_params(reserve_publish_params(2 * 1024 * 1024, 1, 8)),
        reserve_publish_params(reserve_publish_params(2 * 1024 * 1024, 8, 2))));
