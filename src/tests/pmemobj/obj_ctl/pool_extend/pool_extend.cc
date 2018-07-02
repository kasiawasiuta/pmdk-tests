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

#include "pool_extend.h"

ObjCtlPoolExtendTest::ObjCtlPoolExtendTest() {

}

void ObjCtlPoolExtendTest::SetUp() {
  errno = 0;
  ApiC::CreateDirectoryT(pools_dir);
  ApiC::CreateDirectoryT(pool_dir1);
  ApiC::CreateDirectoryT(pool_dir2);
}

void ObjCtlPoolExtendTest::TearDown() {
  while(ApiC::CleanDirectory(test_dir)) {
  }
}

void ObjCtlPoolExtendTest::ReopenAndCheckPool(PMEMobjpool *pop, std::string path) {
  pmemobj_close(pop);
  pop = nullptr;
  pop = pmemobj_open(path.c_str(), layout);

  auto err = pmemobj_errormsg();
  ASSERT_TRUE(nullptr != pop);
  (void)err;
  pmemobj_close(pop);

  int ret = pmemobj_check(path.c_str(), layout);
  ASSERT_EQ(1, ret);
}
