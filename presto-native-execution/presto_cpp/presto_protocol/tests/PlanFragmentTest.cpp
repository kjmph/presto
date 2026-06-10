/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "presto_cpp/main/common/tests/test_json.h"

namespace fs = boost::filesystem;

using namespace facebook::presto;

namespace {

template <typename T>
void testJsonRoundTrip(const std::string& str) {
  json j = json::parse(str);
  T p = j;

  testJsonRoundtrip(j, p);
}

template <typename T>
void testJsonRoundTripFile(const std::string& filename) {
  testJsonRoundTrip<T>(slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      filename)));
}

std::string joinNodeJson() {
  return R"({
    "@type": ".JoinNode",
    "id": "join",
    "type": "INNER",
    "left": {
      "@type": ".ValuesNode",
      "id": "left",
      "outputVariables": [
        {"@type": "variable", "name": "l_orderkey", "type": "bigint"}
      ],
      "rows": []
    },
    "right": {
      "@type": ".ValuesNode",
      "id": "right",
      "outputVariables": [
        {"@type": "variable", "name": "r_orderkey", "type": "bigint"}
      ],
      "rows": []
    },
    "criteria": [
      {
        "left": {"@type": "variable", "name": "l_orderkey", "type": "bigint"},
        "right": {"@type": "variable", "name": "r_orderkey", "type": "bigint"}
      }
    ],
    "outputVariables": [
      {"@type": "variable", "name": "l_orderkey", "type": "bigint"},
      {"@type": "variable", "name": "r_orderkey", "type": "bigint"}
    ],
    "filter": null,
    "leftHashVariable": null,
    "rightHashVariable": null,
    "distributionType": null,
    "dynamicFilters": {},
    "leftKeysUnique": true,
    "rightKeysUnique": true,
    "leftKeysNonNull": true,
    "rightKeysNonNull": false,
    "leftKeysCoveredByRightKeys": true,
    "rightKeysCoveredByLeftKeys": false
  })";
}

std::string semiJoinNodeJson() {
  return R"({
    "@type": ".SemiJoinNode",
    "id": "semi_join",
    "source": {
      "@type": ".ValuesNode",
      "id": "source",
      "outputVariables": [
        {"@type": "variable", "name": "l_orderkey", "type": "bigint"}
      ],
      "rows": []
    },
    "filteringSource": {
      "@type": ".ValuesNode",
      "id": "filtering",
      "outputVariables": [
        {"@type": "variable", "name": "r_orderkey", "type": "bigint"}
      ],
      "rows": []
    },
    "sourceJoinVariable": {"@type": "variable", "name": "l_orderkey", "type": "bigint"},
    "filteringSourceJoinVariable": {"@type": "variable", "name": "r_orderkey", "type": "bigint"},
    "semiJoinOutput": {"@type": "variable", "name": "match", "type": "boolean"},
    "sourceHashVariable": null,
    "filteringSourceHashVariable": null,
    "distributionType": null,
    "dynamicFilters": {},
    "sourceKeyUnique": true,
    "filteringSourceKeyUnique": false,
    "sourceKeyNonNull": true,
    "filteringSourceKeyNonNull": false
  })";
}
} // namespace

class TestPlanNodes : public ::testing::Test {};

TEST_F(TestPlanNodes, TestExchangeNode) {
  testJsonRoundTripFile<protocol::ExchangeNode>("ExchangeNode.json");
}

TEST_F(TestPlanNodes, TestFilterNode) {
  testJsonRoundTripFile<protocol::FilterNode>("FilterNode.json");
}

TEST_F(TestPlanNodes, TestOutputNode) {
  testJsonRoundTripFile<protocol::OutputNode>("OutputNode.json");
}

TEST_F(TestPlanNodes, TestValuesNode) {
  testJsonRoundTripFile<protocol::ValuesNode>("ValuesNode.json");
}

TEST_F(TestPlanNodes, TestJoinNodeUniqueKeys) {
  json j = json::parse(joinNodeJson());
  protocol::JoinNode node = j;

  ASSERT_TRUE(node.leftKeysUnique);
  ASSERT_TRUE(node.rightKeysUnique);
  ASSERT_TRUE(node.leftKeysNonNull);
  ASSERT_FALSE(node.rightKeysNonNull);
  ASSERT_TRUE(node.leftKeysCoveredByRightKeys);
  ASSERT_FALSE(node.rightKeysCoveredByLeftKeys);

  json r = node;
  ASSERT_TRUE(r["leftKeysUnique"].get<bool>());
  ASSERT_TRUE(r["rightKeysUnique"].get<bool>());
  ASSERT_TRUE(r["leftKeysNonNull"].get<bool>());
  ASSERT_FALSE(r["rightKeysNonNull"].get<bool>());
  ASSERT_TRUE(r["leftKeysCoveredByRightKeys"].get<bool>());
  ASSERT_FALSE(r["rightKeysCoveredByLeftKeys"].get<bool>());
  testJsonRoundtrip(j, node);
}

TEST_F(TestPlanNodes, TestJoinNodeUniqueKeysAbsent) {
  json j = json::parse(joinNodeJson());
  j.erase("leftKeysUnique");
  j.erase("rightKeysUnique");
  j.erase("leftKeysNonNull");
  j.erase("rightKeysNonNull");
  j.erase("leftKeysCoveredByRightKeys");
  j.erase("rightKeysCoveredByLeftKeys");

  protocol::JoinNode node = j;
  ASSERT_FALSE(node.leftKeysUnique);
  ASSERT_FALSE(node.rightKeysUnique);
  ASSERT_FALSE(node.leftKeysNonNull);
  ASSERT_FALSE(node.rightKeysNonNull);
  ASSERT_FALSE(node.leftKeysCoveredByRightKeys);
  ASSERT_FALSE(node.rightKeysCoveredByLeftKeys);
}

TEST_F(TestPlanNodes, TestSemiJoinNodeUniqueKeys) {
  json j = json::parse(semiJoinNodeJson());
  protocol::SemiJoinNode node = j;

  ASSERT_TRUE(node.sourceKeyUnique);
  ASSERT_FALSE(node.filteringSourceKeyUnique);
  ASSERT_TRUE(node.sourceKeyNonNull);
  ASSERT_FALSE(node.filteringSourceKeyNonNull);

  json r = node;
  ASSERT_TRUE(r["sourceKeyUnique"].get<bool>());
  ASSERT_FALSE(r["filteringSourceKeyUnique"].get<bool>());
  ASSERT_TRUE(r["sourceKeyNonNull"].get<bool>());
  ASSERT_FALSE(r["filteringSourceKeyNonNull"].get<bool>());
  testJsonRoundtrip(j, node);
}

TEST_F(TestPlanNodes, TestSemiJoinNodeUniqueKeysAbsent) {
  json j = json::parse(semiJoinNodeJson());
  j.erase("sourceKeyUnique");
  j.erase("filteringSourceKeyUnique");
  j.erase("sourceKeyNonNull");
  j.erase("filteringSourceKeyNonNull");

  protocol::SemiJoinNode node = j;
  ASSERT_FALSE(node.sourceKeyUnique);
  ASSERT_FALSE(node.filteringSourceKeyUnique);
  ASSERT_FALSE(node.sourceKeyNonNull);
  ASSERT_FALSE(node.filteringSourceKeyNonNull);
}

TEST_F(TestPlanNodes, TestRemoteSourceNodeTransportTypeAny) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "RemoteSourceNodeAny.json"));
  json j = json::parse(str);
  protocol::RemoteSourceNode node = j;

  ASSERT_NE(node.transportType, nullptr);
  ASSERT_EQ(*node.transportType, protocol::TransportType::ANY);

  // Round-trip: serialize back to JSON and verify transportType is preserved.
  json r = node;
  ASSERT_EQ(r["transportType"], "ANY");
  testJsonRoundtrip(j, node);
}

TEST_F(TestPlanNodes, TestRemoteSourceNodeTransportTypeHttp) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "RemoteSourceNodeHttp.json"));
  json j = json::parse(str);
  protocol::RemoteSourceNode node = j;

  ASSERT_NE(node.transportType, nullptr);
  ASSERT_EQ(*node.transportType, protocol::TransportType::HTTP);

  json r = node;
  ASSERT_EQ(r["transportType"], "HTTP");
  testJsonRoundtrip(j, node);
}

TEST_F(TestPlanNodes, TestRemoteSourceNodeTransportTypeAbsent) {
  // RemoteSourceNode JSON without transportType field should leave ptr null.
  std::string str = R"({
    "@type": "com.facebook.presto.sql.planner.plan.RemoteSourceNode",
    "id": "42",
    "sourceFragmentIds": ["1"],
    "outputVariables": [
      {"@type": "variable", "name": "col", "type": "bigint"}
    ],
    "ensureSourceOrdering": false,
    "exchangeType": "GATHER",
    "encoding": "COLUMNAR"
  })";
  json j = json::parse(str);
  protocol::RemoteSourceNode node = j;

  ASSERT_EQ(node.transportType, nullptr);
}

TEST_F(TestPlanNodes, TestPlanFragmentOutputTransportTypeAny) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "PlanFragmentWithRemoteSource.json"));
  json j = json::parse(str);
  j["outputTransportType"] = "ANY";

  protocol::PlanFragment fragment = j;
  ASSERT_NE(fragment.outputTransportType, nullptr);
  ASSERT_EQ(*fragment.outputTransportType, protocol::TransportType::ANY);

  json r = fragment;
  ASSERT_EQ(r["outputTransportType"], "ANY");
}

TEST_F(TestPlanNodes, TestPlanFragmentOutputTransportTypeHttp) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "PlanFragmentWithRemoteSource.json"));
  json j = json::parse(str);
  j["outputTransportType"] = "HTTP";

  protocol::PlanFragment fragment = j;
  ASSERT_NE(fragment.outputTransportType, nullptr);
  ASSERT_EQ(*fragment.outputTransportType, protocol::TransportType::HTTP);

  json r = fragment;
  ASSERT_EQ(r["outputTransportType"], "HTTP");
}

TEST_F(TestPlanNodes, TestPlanFragmentOutputTransportTypeAbsent) {
  // PlanFragment JSON without outputTransportType should leave ptr null.
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "PlanFragmentWithRemoteSource.json"));
  json j = json::parse(str);
  // Ensure the field is NOT present.
  ASSERT_FALSE(j.count("outputTransportType"));

  protocol::PlanFragment fragment = j;
  ASSERT_EQ(fragment.outputTransportType, nullptr);
}
