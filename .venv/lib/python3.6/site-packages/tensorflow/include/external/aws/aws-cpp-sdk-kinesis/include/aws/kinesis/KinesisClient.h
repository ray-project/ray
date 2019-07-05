/*
* Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
*  http://aws.amazon.com/apache2.0
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

#pragma once
#include <aws/kinesis/Kinesis_EXPORTS.h>
#include <aws/kinesis/KinesisErrors.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/AWSClient.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/kinesis/model/DescribeLimitsResult.h>
#include <aws/kinesis/model/DescribeStreamResult.h>
#include <aws/kinesis/model/DescribeStreamSummaryResult.h>
#include <aws/kinesis/model/DisableEnhancedMonitoringResult.h>
#include <aws/kinesis/model/EnableEnhancedMonitoringResult.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/ListStreamsResult.h>
#include <aws/kinesis/model/ListTagsForStreamResult.h>
#include <aws/kinesis/model/PutRecordResult.h>
#include <aws/kinesis/model/PutRecordsResult.h>
#include <aws/kinesis/model/UpdateShardCountResult.h>
#include <aws/core/NoResult.h>
#include <aws/core/client/AsyncCallerContext.h>
#include <aws/core/http/HttpTypes.h>
#include <future>
#include <functional>

namespace Aws
{

namespace Http
{
  class HttpClient;
  class HttpClientFactory;
} // namespace Http

namespace Utils
{
  template< typename R, typename E> class Outcome;

namespace Threading
{
  class Executor;
} // namespace Threading

namespace Json
{
  class JsonValue;
} // namespace Json
} // namespace Utils

namespace Auth
{
  class AWSCredentials;
  class AWSCredentialsProvider;
} // namespace Auth

namespace Client
{
  class RetryStrategy;
} // namespace Client

namespace Kinesis
{

namespace Model
{
        class AddTagsToStreamRequest;
        class CreateStreamRequest;
        class DecreaseStreamRetentionPeriodRequest;
        class DeleteStreamRequest;
        class DescribeLimitsRequest;
        class DescribeStreamRequest;
        class DescribeStreamSummaryRequest;
        class DisableEnhancedMonitoringRequest;
        class EnableEnhancedMonitoringRequest;
        class GetRecordsRequest;
        class GetShardIteratorRequest;
        class IncreaseStreamRetentionPeriodRequest;
        class ListStreamsRequest;
        class ListTagsForStreamRequest;
        class MergeShardsRequest;
        class PutRecordRequest;
        class PutRecordsRequest;
        class RemoveTagsFromStreamRequest;
        class SplitShardRequest;
        class StartStreamEncryptionRequest;
        class StopStreamEncryptionRequest;
        class UpdateShardCountRequest;

        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> AddTagsToStreamOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> CreateStreamOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> DecreaseStreamRetentionPeriodOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> DeleteStreamOutcome;
        typedef Aws::Utils::Outcome<DescribeLimitsResult, Aws::Client::AWSError<KinesisErrors>> DescribeLimitsOutcome;
        typedef Aws::Utils::Outcome<DescribeStreamResult, Aws::Client::AWSError<KinesisErrors>> DescribeStreamOutcome;
        typedef Aws::Utils::Outcome<DescribeStreamSummaryResult, Aws::Client::AWSError<KinesisErrors>> DescribeStreamSummaryOutcome;
        typedef Aws::Utils::Outcome<DisableEnhancedMonitoringResult, Aws::Client::AWSError<KinesisErrors>> DisableEnhancedMonitoringOutcome;
        typedef Aws::Utils::Outcome<EnableEnhancedMonitoringResult, Aws::Client::AWSError<KinesisErrors>> EnableEnhancedMonitoringOutcome;
        typedef Aws::Utils::Outcome<GetRecordsResult, Aws::Client::AWSError<KinesisErrors>> GetRecordsOutcome;
        typedef Aws::Utils::Outcome<GetShardIteratorResult, Aws::Client::AWSError<KinesisErrors>> GetShardIteratorOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> IncreaseStreamRetentionPeriodOutcome;
        typedef Aws::Utils::Outcome<ListStreamsResult, Aws::Client::AWSError<KinesisErrors>> ListStreamsOutcome;
        typedef Aws::Utils::Outcome<ListTagsForStreamResult, Aws::Client::AWSError<KinesisErrors>> ListTagsForStreamOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> MergeShardsOutcome;
        typedef Aws::Utils::Outcome<PutRecordResult, Aws::Client::AWSError<KinesisErrors>> PutRecordOutcome;
        typedef Aws::Utils::Outcome<PutRecordsResult, Aws::Client::AWSError<KinesisErrors>> PutRecordsOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> RemoveTagsFromStreamOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> SplitShardOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> StartStreamEncryptionOutcome;
        typedef Aws::Utils::Outcome<Aws::NoResult, Aws::Client::AWSError<KinesisErrors>> StopStreamEncryptionOutcome;
        typedef Aws::Utils::Outcome<UpdateShardCountResult, Aws::Client::AWSError<KinesisErrors>> UpdateShardCountOutcome;

        typedef std::future<AddTagsToStreamOutcome> AddTagsToStreamOutcomeCallable;
        typedef std::future<CreateStreamOutcome> CreateStreamOutcomeCallable;
        typedef std::future<DecreaseStreamRetentionPeriodOutcome> DecreaseStreamRetentionPeriodOutcomeCallable;
        typedef std::future<DeleteStreamOutcome> DeleteStreamOutcomeCallable;
        typedef std::future<DescribeLimitsOutcome> DescribeLimitsOutcomeCallable;
        typedef std::future<DescribeStreamOutcome> DescribeStreamOutcomeCallable;
        typedef std::future<DescribeStreamSummaryOutcome> DescribeStreamSummaryOutcomeCallable;
        typedef std::future<DisableEnhancedMonitoringOutcome> DisableEnhancedMonitoringOutcomeCallable;
        typedef std::future<EnableEnhancedMonitoringOutcome> EnableEnhancedMonitoringOutcomeCallable;
        typedef std::future<GetRecordsOutcome> GetRecordsOutcomeCallable;
        typedef std::future<GetShardIteratorOutcome> GetShardIteratorOutcomeCallable;
        typedef std::future<IncreaseStreamRetentionPeriodOutcome> IncreaseStreamRetentionPeriodOutcomeCallable;
        typedef std::future<ListStreamsOutcome> ListStreamsOutcomeCallable;
        typedef std::future<ListTagsForStreamOutcome> ListTagsForStreamOutcomeCallable;
        typedef std::future<MergeShardsOutcome> MergeShardsOutcomeCallable;
        typedef std::future<PutRecordOutcome> PutRecordOutcomeCallable;
        typedef std::future<PutRecordsOutcome> PutRecordsOutcomeCallable;
        typedef std::future<RemoveTagsFromStreamOutcome> RemoveTagsFromStreamOutcomeCallable;
        typedef std::future<SplitShardOutcome> SplitShardOutcomeCallable;
        typedef std::future<StartStreamEncryptionOutcome> StartStreamEncryptionOutcomeCallable;
        typedef std::future<StopStreamEncryptionOutcome> StopStreamEncryptionOutcomeCallable;
        typedef std::future<UpdateShardCountOutcome> UpdateShardCountOutcomeCallable;
} // namespace Model

  class KinesisClient;

    typedef std::function<void(const KinesisClient*, const Model::AddTagsToStreamRequest&, const Model::AddTagsToStreamOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > AddTagsToStreamResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::CreateStreamRequest&, const Model::CreateStreamOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > CreateStreamResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::DecreaseStreamRetentionPeriodRequest&, const Model::DecreaseStreamRetentionPeriodOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DecreaseStreamRetentionPeriodResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::DeleteStreamRequest&, const Model::DeleteStreamOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DeleteStreamResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::DescribeLimitsRequest&, const Model::DescribeLimitsOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DescribeLimitsResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::DescribeStreamRequest&, const Model::DescribeStreamOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DescribeStreamResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::DescribeStreamSummaryRequest&, const Model::DescribeStreamSummaryOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DescribeStreamSummaryResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::DisableEnhancedMonitoringRequest&, const Model::DisableEnhancedMonitoringOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > DisableEnhancedMonitoringResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::EnableEnhancedMonitoringRequest&, const Model::EnableEnhancedMonitoringOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > EnableEnhancedMonitoringResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::GetRecordsRequest&, const Model::GetRecordsOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetRecordsResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::GetShardIteratorRequest&, const Model::GetShardIteratorOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > GetShardIteratorResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::IncreaseStreamRetentionPeriodRequest&, const Model::IncreaseStreamRetentionPeriodOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > IncreaseStreamRetentionPeriodResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::ListStreamsRequest&, const Model::ListStreamsOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ListStreamsResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::ListTagsForStreamRequest&, const Model::ListTagsForStreamOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > ListTagsForStreamResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::MergeShardsRequest&, const Model::MergeShardsOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > MergeShardsResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::PutRecordRequest&, const Model::PutRecordOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > PutRecordResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::PutRecordsRequest&, const Model::PutRecordsOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > PutRecordsResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::RemoveTagsFromStreamRequest&, const Model::RemoveTagsFromStreamOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > RemoveTagsFromStreamResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::SplitShardRequest&, const Model::SplitShardOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > SplitShardResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::StartStreamEncryptionRequest&, const Model::StartStreamEncryptionOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > StartStreamEncryptionResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::StopStreamEncryptionRequest&, const Model::StopStreamEncryptionOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > StopStreamEncryptionResponseReceivedHandler;
    typedef std::function<void(const KinesisClient*, const Model::UpdateShardCountRequest&, const Model::UpdateShardCountOutcome&, const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) > UpdateShardCountResponseReceivedHandler;

  /**
   * <fullname>Amazon Kinesis Streams Service API Reference</fullname> <p>Amazon
   * Kinesis Streams is a managed service that scales elastically for real time
   * processing of streaming big data.</p>
   */
  class AWS_KINESIS_API KinesisClient : public Aws::Client::AWSJsonClient
  {
    public:
      typedef Aws::Client::AWSJsonClient BASECLASS;

       /**
        * Initializes client to use DefaultCredentialProviderChain, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        KinesisClient(const Aws::Client::ClientConfiguration& clientConfiguration = Aws::Client::ClientConfiguration());

       /**
        * Initializes client to use SimpleAWSCredentialsProvider, with default http client factory, and optional client config. If client config
        * is not specified, it will be initialized to default values.
        */
        KinesisClient(const Aws::Auth::AWSCredentials& credentials, const Aws::Client::ClientConfiguration& clientConfiguration = Aws::Client::ClientConfiguration());

       /**
        * Initializes client to use specified credentials provider with specified client config. If http client factory is not supplied,
        * the default http client factory will be used
        */
        KinesisClient(const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentialsProvider,
            const Aws::Client::ClientConfiguration& clientConfiguration = Aws::Client::ClientConfiguration());

        virtual ~KinesisClient();

        inline virtual const char* GetServiceClientName() const override { return "kinesis"; }


        /**
         * <p>Adds or updates tags for the specified Kinesis stream. Each stream can have
         * up to 10 tags.</p> <p>If tags have already been assigned to the stream,
         * <code>AddTagsToStream</code> overwrites any existing tags that correspond to the
         * specified tag keys.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/AddTagsToStream">AWS
         * API Reference</a></p>
         */
        virtual Model::AddTagsToStreamOutcome AddTagsToStream(const Model::AddTagsToStreamRequest& request) const;

        /**
         * <p>Adds or updates tags for the specified Kinesis stream. Each stream can have
         * up to 10 tags.</p> <p>If tags have already been assigned to the stream,
         * <code>AddTagsToStream</code> overwrites any existing tags that correspond to the
         * specified tag keys.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/AddTagsToStream">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::AddTagsToStreamOutcomeCallable AddTagsToStreamCallable(const Model::AddTagsToStreamRequest& request) const;

        /**
         * <p>Adds or updates tags for the specified Kinesis stream. Each stream can have
         * up to 10 tags.</p> <p>If tags have already been assigned to the stream,
         * <code>AddTagsToStream</code> overwrites any existing tags that correspond to the
         * specified tag keys.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/AddTagsToStream">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void AddTagsToStreamAsync(const Model::AddTagsToStreamRequest& request, const AddTagsToStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Creates a Kinesis stream. A stream captures and transports data records that
         * are continuously emitted from different data sources or <i>producers</i>.
         * Scale-out within a stream is explicitly supported by means of shards, which are
         * uniquely identified groups of data records in a stream.</p> <p>You specify and
         * control the number of shards that a stream is composed of. Each shard can
         * support reads up to 5 transactions per second, up to a maximum data read total
         * of 2 MB per second. Each shard can support writes up to 1,000 records per
         * second, up to a maximum data write total of 1 MB per second. I the amount of
         * data input increases or decreases, you can add or remove shards.</p> <p>The
         * stream name identifies the stream. The name is scoped to the AWS account used by
         * the application. It is also scoped by region. That is, two streams in two
         * different accounts can have the same name, and two streams in the same account,
         * but in two different regions, can have the same name.</p> <p>
         * <code>CreateStream</code> is an asynchronous operation. Upon receiving a
         * <code>CreateStream</code> request, Kinesis Streams immediately returns and sets
         * the stream status to <code>CREATING</code>. After the stream is created, Kinesis
         * Streams sets the stream status to <code>ACTIVE</code>. You should perform read
         * and write operations only on an <code>ACTIVE</code> stream. </p> <p>You receive
         * a <code>LimitExceededException</code> when making a <code>CreateStream</code>
         * request when you try to do one of the following:</p> <ul> <li> <p>Have more than
         * five streams in the <code>CREATING</code> state at any point in time.</p> </li>
         * <li> <p>Create more shards than are authorized for your account.</p> </li> </ul>
         * <p>For the default shard limit for an AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase
         * this limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p> <p>You can use <code>DescribeStream</code> to check the
         * stream status, which is returned in <code>StreamStatus</code>.</p> <p>
         * <a>CreateStream</a> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/CreateStream">AWS
         * API Reference</a></p>
         */
        virtual Model::CreateStreamOutcome CreateStream(const Model::CreateStreamRequest& request) const;

        /**
         * <p>Creates a Kinesis stream. A stream captures and transports data records that
         * are continuously emitted from different data sources or <i>producers</i>.
         * Scale-out within a stream is explicitly supported by means of shards, which are
         * uniquely identified groups of data records in a stream.</p> <p>You specify and
         * control the number of shards that a stream is composed of. Each shard can
         * support reads up to 5 transactions per second, up to a maximum data read total
         * of 2 MB per second. Each shard can support writes up to 1,000 records per
         * second, up to a maximum data write total of 1 MB per second. I the amount of
         * data input increases or decreases, you can add or remove shards.</p> <p>The
         * stream name identifies the stream. The name is scoped to the AWS account used by
         * the application. It is also scoped by region. That is, two streams in two
         * different accounts can have the same name, and two streams in the same account,
         * but in two different regions, can have the same name.</p> <p>
         * <code>CreateStream</code> is an asynchronous operation. Upon receiving a
         * <code>CreateStream</code> request, Kinesis Streams immediately returns and sets
         * the stream status to <code>CREATING</code>. After the stream is created, Kinesis
         * Streams sets the stream status to <code>ACTIVE</code>. You should perform read
         * and write operations only on an <code>ACTIVE</code> stream. </p> <p>You receive
         * a <code>LimitExceededException</code> when making a <code>CreateStream</code>
         * request when you try to do one of the following:</p> <ul> <li> <p>Have more than
         * five streams in the <code>CREATING</code> state at any point in time.</p> </li>
         * <li> <p>Create more shards than are authorized for your account.</p> </li> </ul>
         * <p>For the default shard limit for an AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase
         * this limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p> <p>You can use <code>DescribeStream</code> to check the
         * stream status, which is returned in <code>StreamStatus</code>.</p> <p>
         * <a>CreateStream</a> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/CreateStream">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::CreateStreamOutcomeCallable CreateStreamCallable(const Model::CreateStreamRequest& request) const;

        /**
         * <p>Creates a Kinesis stream. A stream captures and transports data records that
         * are continuously emitted from different data sources or <i>producers</i>.
         * Scale-out within a stream is explicitly supported by means of shards, which are
         * uniquely identified groups of data records in a stream.</p> <p>You specify and
         * control the number of shards that a stream is composed of. Each shard can
         * support reads up to 5 transactions per second, up to a maximum data read total
         * of 2 MB per second. Each shard can support writes up to 1,000 records per
         * second, up to a maximum data write total of 1 MB per second. I the amount of
         * data input increases or decreases, you can add or remove shards.</p> <p>The
         * stream name identifies the stream. The name is scoped to the AWS account used by
         * the application. It is also scoped by region. That is, two streams in two
         * different accounts can have the same name, and two streams in the same account,
         * but in two different regions, can have the same name.</p> <p>
         * <code>CreateStream</code> is an asynchronous operation. Upon receiving a
         * <code>CreateStream</code> request, Kinesis Streams immediately returns and sets
         * the stream status to <code>CREATING</code>. After the stream is created, Kinesis
         * Streams sets the stream status to <code>ACTIVE</code>. You should perform read
         * and write operations only on an <code>ACTIVE</code> stream. </p> <p>You receive
         * a <code>LimitExceededException</code> when making a <code>CreateStream</code>
         * request when you try to do one of the following:</p> <ul> <li> <p>Have more than
         * five streams in the <code>CREATING</code> state at any point in time.</p> </li>
         * <li> <p>Create more shards than are authorized for your account.</p> </li> </ul>
         * <p>For the default shard limit for an AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase
         * this limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p> <p>You can use <code>DescribeStream</code> to check the
         * stream status, which is returned in <code>StreamStatus</code>.</p> <p>
         * <a>CreateStream</a> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/CreateStream">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void CreateStreamAsync(const Model::CreateStreamRequest& request, const CreateStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Decreases the Kinesis stream's retention period, which is the length of time
         * data records are accessible after they are added to the stream. The minimum
         * value of a stream's retention period is 24 hours.</p> <p>This operation may
         * result in lost data. For example, if the stream's retention period is 48 hours
         * and is decreased to 24 hours, any data already in the stream that is older than
         * 24 hours is inaccessible.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DecreaseStreamRetentionPeriod">AWS
         * API Reference</a></p>
         */
        virtual Model::DecreaseStreamRetentionPeriodOutcome DecreaseStreamRetentionPeriod(const Model::DecreaseStreamRetentionPeriodRequest& request) const;

        /**
         * <p>Decreases the Kinesis stream's retention period, which is the length of time
         * data records are accessible after they are added to the stream. The minimum
         * value of a stream's retention period is 24 hours.</p> <p>This operation may
         * result in lost data. For example, if the stream's retention period is 48 hours
         * and is decreased to 24 hours, any data already in the stream that is older than
         * 24 hours is inaccessible.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DecreaseStreamRetentionPeriod">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::DecreaseStreamRetentionPeriodOutcomeCallable DecreaseStreamRetentionPeriodCallable(const Model::DecreaseStreamRetentionPeriodRequest& request) const;

        /**
         * <p>Decreases the Kinesis stream's retention period, which is the length of time
         * data records are accessible after they are added to the stream. The minimum
         * value of a stream's retention period is 24 hours.</p> <p>This operation may
         * result in lost data. For example, if the stream's retention period is 48 hours
         * and is decreased to 24 hours, any data already in the stream that is older than
         * 24 hours is inaccessible.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DecreaseStreamRetentionPeriod">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void DecreaseStreamRetentionPeriodAsync(const Model::DecreaseStreamRetentionPeriodRequest& request, const DecreaseStreamRetentionPeriodResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Deletes a Kinesis stream and all its shards and data. You must shut down any
         * applications that are operating on the stream before you delete the stream. If
         * an application attempts to operate on a deleted stream, it receives the
         * exception <code>ResourceNotFoundException</code>.</p> <p>If the stream is in the
         * <code>ACTIVE</code> state, you can delete it. After a <code>DeleteStream</code>
         * request, the specified stream is in the <code>DELETING</code> state until
         * Kinesis Streams completes the deletion.</p> <p> <b>Note:</b> Kinesis Streams
         * might continue to accept data read and write operations, such as
         * <a>PutRecord</a>, <a>PutRecords</a>, and <a>GetRecords</a>, on a stream in the
         * <code>DELETING</code> state until the stream deletion is complete.</p> <p>When
         * you delete a stream, any shards in that stream are also deleted, and any tags
         * are dissociated from the stream.</p> <p>You can use the <a>DescribeStream</a>
         * operation to check the state of the stream, which is returned in
         * <code>StreamStatus</code>.</p> <p> <a>DeleteStream</a> has a limit of 5
         * transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DeleteStream">AWS
         * API Reference</a></p>
         */
        virtual Model::DeleteStreamOutcome DeleteStream(const Model::DeleteStreamRequest& request) const;

        /**
         * <p>Deletes a Kinesis stream and all its shards and data. You must shut down any
         * applications that are operating on the stream before you delete the stream. If
         * an application attempts to operate on a deleted stream, it receives the
         * exception <code>ResourceNotFoundException</code>.</p> <p>If the stream is in the
         * <code>ACTIVE</code> state, you can delete it. After a <code>DeleteStream</code>
         * request, the specified stream is in the <code>DELETING</code> state until
         * Kinesis Streams completes the deletion.</p> <p> <b>Note:</b> Kinesis Streams
         * might continue to accept data read and write operations, such as
         * <a>PutRecord</a>, <a>PutRecords</a>, and <a>GetRecords</a>, on a stream in the
         * <code>DELETING</code> state until the stream deletion is complete.</p> <p>When
         * you delete a stream, any shards in that stream are also deleted, and any tags
         * are dissociated from the stream.</p> <p>You can use the <a>DescribeStream</a>
         * operation to check the state of the stream, which is returned in
         * <code>StreamStatus</code>.</p> <p> <a>DeleteStream</a> has a limit of 5
         * transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DeleteStream">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::DeleteStreamOutcomeCallable DeleteStreamCallable(const Model::DeleteStreamRequest& request) const;

        /**
         * <p>Deletes a Kinesis stream and all its shards and data. You must shut down any
         * applications that are operating on the stream before you delete the stream. If
         * an application attempts to operate on a deleted stream, it receives the
         * exception <code>ResourceNotFoundException</code>.</p> <p>If the stream is in the
         * <code>ACTIVE</code> state, you can delete it. After a <code>DeleteStream</code>
         * request, the specified stream is in the <code>DELETING</code> state until
         * Kinesis Streams completes the deletion.</p> <p> <b>Note:</b> Kinesis Streams
         * might continue to accept data read and write operations, such as
         * <a>PutRecord</a>, <a>PutRecords</a>, and <a>GetRecords</a>, on a stream in the
         * <code>DELETING</code> state until the stream deletion is complete.</p> <p>When
         * you delete a stream, any shards in that stream are also deleted, and any tags
         * are dissociated from the stream.</p> <p>You can use the <a>DescribeStream</a>
         * operation to check the state of the stream, which is returned in
         * <code>StreamStatus</code>.</p> <p> <a>DeleteStream</a> has a limit of 5
         * transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DeleteStream">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void DeleteStreamAsync(const Model::DeleteStreamRequest& request, const DeleteStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Describes the shard limits and usage for the account.</p> <p>If you update
         * your account limits, the old limits might be returned for a few minutes.</p>
         * <p>This operation has a limit of 1 transaction per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeLimits">AWS
         * API Reference</a></p>
         */
        virtual Model::DescribeLimitsOutcome DescribeLimits(const Model::DescribeLimitsRequest& request) const;

        /**
         * <p>Describes the shard limits and usage for the account.</p> <p>If you update
         * your account limits, the old limits might be returned for a few minutes.</p>
         * <p>This operation has a limit of 1 transaction per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeLimits">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::DescribeLimitsOutcomeCallable DescribeLimitsCallable(const Model::DescribeLimitsRequest& request) const;

        /**
         * <p>Describes the shard limits and usage for the account.</p> <p>If you update
         * your account limits, the old limits might be returned for a few minutes.</p>
         * <p>This operation has a limit of 1 transaction per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeLimits">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void DescribeLimitsAsync(const Model::DescribeLimitsRequest& request, const DescribeLimitsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Describes the specified Kinesis stream.</p> <p>The information returned
         * includes the stream name, Amazon Resource Name (ARN), creation time, enhanced
         * metric configuration, and shard map. The shard map is an array of shard objects.
         * For each shard object, there is the hash key and sequence number ranges that the
         * shard spans, and the IDs of any earlier shards that played in a role in creating
         * the shard. Every record ingested in the stream is identified by a sequence
         * number, which is assigned when the record is put into the stream.</p> <p>You can
         * limit the number of shards returned by each call. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-retrieve-shards.html">Retrieving
         * Shards from a Stream</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>There are no guarantees about the chronological order shards
         * returned. To process shards in chronological order, use the ID of the parent
         * shard to track the lineage to the oldest shard.</p> <p>This operation has a
         * limit of 10 transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeStream">AWS
         * API Reference</a></p>
         */
        virtual Model::DescribeStreamOutcome DescribeStream(const Model::DescribeStreamRequest& request) const;

        /**
         * <p>Describes the specified Kinesis stream.</p> <p>The information returned
         * includes the stream name, Amazon Resource Name (ARN), creation time, enhanced
         * metric configuration, and shard map. The shard map is an array of shard objects.
         * For each shard object, there is the hash key and sequence number ranges that the
         * shard spans, and the IDs of any earlier shards that played in a role in creating
         * the shard. Every record ingested in the stream is identified by a sequence
         * number, which is assigned when the record is put into the stream.</p> <p>You can
         * limit the number of shards returned by each call. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-retrieve-shards.html">Retrieving
         * Shards from a Stream</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>There are no guarantees about the chronological order shards
         * returned. To process shards in chronological order, use the ID of the parent
         * shard to track the lineage to the oldest shard.</p> <p>This operation has a
         * limit of 10 transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeStream">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::DescribeStreamOutcomeCallable DescribeStreamCallable(const Model::DescribeStreamRequest& request) const;

        /**
         * <p>Describes the specified Kinesis stream.</p> <p>The information returned
         * includes the stream name, Amazon Resource Name (ARN), creation time, enhanced
         * metric configuration, and shard map. The shard map is an array of shard objects.
         * For each shard object, there is the hash key and sequence number ranges that the
         * shard spans, and the IDs of any earlier shards that played in a role in creating
         * the shard. Every record ingested in the stream is identified by a sequence
         * number, which is assigned when the record is put into the stream.</p> <p>You can
         * limit the number of shards returned by each call. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-retrieve-shards.html">Retrieving
         * Shards from a Stream</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>There are no guarantees about the chronological order shards
         * returned. To process shards in chronological order, use the ID of the parent
         * shard to track the lineage to the oldest shard.</p> <p>This operation has a
         * limit of 10 transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeStream">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void DescribeStreamAsync(const Model::DescribeStreamRequest& request, const DescribeStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Provides a summarized description of the specified Kinesis stream without the
         * shard list.</p> <p>The information returned includes the stream name, Amazon
         * Resource Name (ARN), status, record retention period, approximate creation time,
         * monitoring, encryption details, and open shard count. </p><p><h3>See Also:</h3> 
         * <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeStreamSummary">AWS
         * API Reference</a></p>
         */
        virtual Model::DescribeStreamSummaryOutcome DescribeStreamSummary(const Model::DescribeStreamSummaryRequest& request) const;

        /**
         * <p>Provides a summarized description of the specified Kinesis stream without the
         * shard list.</p> <p>The information returned includes the stream name, Amazon
         * Resource Name (ARN), status, record retention period, approximate creation time,
         * monitoring, encryption details, and open shard count. </p><p><h3>See Also:</h3> 
         * <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeStreamSummary">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::DescribeStreamSummaryOutcomeCallable DescribeStreamSummaryCallable(const Model::DescribeStreamSummaryRequest& request) const;

        /**
         * <p>Provides a summarized description of the specified Kinesis stream without the
         * shard list.</p> <p>The information returned includes the stream name, Amazon
         * Resource Name (ARN), status, record retention period, approximate creation time,
         * monitoring, encryption details, and open shard count. </p><p><h3>See Also:</h3> 
         * <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DescribeStreamSummary">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void DescribeStreamSummaryAsync(const Model::DescribeStreamSummaryRequest& request, const DescribeStreamSummaryResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Disables enhanced monitoring.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DisableEnhancedMonitoring">AWS
         * API Reference</a></p>
         */
        virtual Model::DisableEnhancedMonitoringOutcome DisableEnhancedMonitoring(const Model::DisableEnhancedMonitoringRequest& request) const;

        /**
         * <p>Disables enhanced monitoring.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DisableEnhancedMonitoring">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::DisableEnhancedMonitoringOutcomeCallable DisableEnhancedMonitoringCallable(const Model::DisableEnhancedMonitoringRequest& request) const;

        /**
         * <p>Disables enhanced monitoring.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/DisableEnhancedMonitoring">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void DisableEnhancedMonitoringAsync(const Model::DisableEnhancedMonitoringRequest& request, const DisableEnhancedMonitoringResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Enables enhanced Kinesis stream monitoring for shard-level
         * metrics.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/EnableEnhancedMonitoring">AWS
         * API Reference</a></p>
         */
        virtual Model::EnableEnhancedMonitoringOutcome EnableEnhancedMonitoring(const Model::EnableEnhancedMonitoringRequest& request) const;

        /**
         * <p>Enables enhanced Kinesis stream monitoring for shard-level
         * metrics.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/EnableEnhancedMonitoring">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::EnableEnhancedMonitoringOutcomeCallable EnableEnhancedMonitoringCallable(const Model::EnableEnhancedMonitoringRequest& request) const;

        /**
         * <p>Enables enhanced Kinesis stream monitoring for shard-level
         * metrics.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/EnableEnhancedMonitoring">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void EnableEnhancedMonitoringAsync(const Model::EnableEnhancedMonitoringRequest& request, const EnableEnhancedMonitoringResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Gets data records from a Kinesis stream's shard.</p> <p>Specify a shard
         * iterator using the <code>ShardIterator</code> parameter. The shard iterator
         * specifies the position in the shard from which you want to start reading data
         * records sequentially. If there are no records available in the portion of the
         * shard that the iterator points to, <a>GetRecords</a> returns an empty list. It
         * might take multiple calls to get to a portion of the shard that contains
         * records.</p> <p>You can scale by provisioning multiple shards per stream while
         * considering service limits (for more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>). Your
         * application should have one thread per shard, each reading continuously from its
         * stream. To read from a stream continually, call <a>GetRecords</a> in a loop. Use
         * <a>GetShardIterator</a> to get the shard iterator to specify in the first
         * <a>GetRecords</a> call. <a>GetRecords</a> returns a new shard iterator in
         * <code>NextShardIterator</code>. Specify the shard iterator returned in
         * <code>NextShardIterator</code> in subsequent calls to <a>GetRecords</a>. If the
         * shard has been closed, the shard iterator can't return more data and
         * <a>GetRecords</a> returns <code>null</code> in <code>NextShardIterator</code>.
         * You can terminate the loop when the shard is closed, or when the shard iterator
         * reaches the record with the sequence number or other attribute that marks it as
         * the last record to process.</p> <p>Each data record can be up to 1 MB in size,
         * and each shard can read up to 2 MB per second. You can ensure that your calls
         * don't exceed the maximum supported size or throughput by using the
         * <code>Limit</code> parameter to specify the maximum number of records that
         * <a>GetRecords</a> can return. Consider your average record size when determining
         * this limit.</p> <p>The size of the data returned by <a>GetRecords</a> varies
         * depending on the utilization of the shard. The maximum size of data that
         * <a>GetRecords</a> can return is 10 MB. If a call returns this amount of data,
         * subsequent calls made within the next 5 seconds throw
         * <code>ProvisionedThroughputExceededException</code>. If there is insufficient
         * provisioned throughput on the shard, subsequent calls made within the next 1
         * second throw <code>ProvisionedThroughputExceededException</code>.
         * <a>GetRecords</a> won't return any data when it throws an exception. For this
         * reason, we recommend that you wait one second between calls to
         * <a>GetRecords</a>; however, it's possible that the application will get
         * exceptions for longer than 1 second.</p> <p>To detect whether the application is
         * falling behind in processing, you can use the <code>MillisBehindLatest</code>
         * response attribute. You can also monitor the stream using CloudWatch metrics and
         * other mechanisms (see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring.html">Monitoring</a>
         * in the <i>Amazon Kinesis Streams Developer Guide</i>).</p> <p>Each Amazon
         * Kinesis record includes a value, <code>ApproximateArrivalTimestamp</code>, that
         * is set when a stream successfully receives and stores a record. This is commonly
         * referred to as a server-side time stamp, whereas a client-side time stamp is set
         * when a data producer creates or sends the record to a stream (a data producer is
         * any data source putting data records into a stream, for example with
         * <a>PutRecords</a>). The time stamp has millisecond precision. There are no
         * guarantees about the time stamp accuracy, or that the time stamp is always
         * increasing. For example, records in a shard or across a stream might have time
         * stamps that are out of order.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetRecords">AWS
         * API Reference</a></p>
         */
        virtual Model::GetRecordsOutcome GetRecords(const Model::GetRecordsRequest& request) const;

        /**
         * <p>Gets data records from a Kinesis stream's shard.</p> <p>Specify a shard
         * iterator using the <code>ShardIterator</code> parameter. The shard iterator
         * specifies the position in the shard from which you want to start reading data
         * records sequentially. If there are no records available in the portion of the
         * shard that the iterator points to, <a>GetRecords</a> returns an empty list. It
         * might take multiple calls to get to a portion of the shard that contains
         * records.</p> <p>You can scale by provisioning multiple shards per stream while
         * considering service limits (for more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>). Your
         * application should have one thread per shard, each reading continuously from its
         * stream. To read from a stream continually, call <a>GetRecords</a> in a loop. Use
         * <a>GetShardIterator</a> to get the shard iterator to specify in the first
         * <a>GetRecords</a> call. <a>GetRecords</a> returns a new shard iterator in
         * <code>NextShardIterator</code>. Specify the shard iterator returned in
         * <code>NextShardIterator</code> in subsequent calls to <a>GetRecords</a>. If the
         * shard has been closed, the shard iterator can't return more data and
         * <a>GetRecords</a> returns <code>null</code> in <code>NextShardIterator</code>.
         * You can terminate the loop when the shard is closed, or when the shard iterator
         * reaches the record with the sequence number or other attribute that marks it as
         * the last record to process.</p> <p>Each data record can be up to 1 MB in size,
         * and each shard can read up to 2 MB per second. You can ensure that your calls
         * don't exceed the maximum supported size or throughput by using the
         * <code>Limit</code> parameter to specify the maximum number of records that
         * <a>GetRecords</a> can return. Consider your average record size when determining
         * this limit.</p> <p>The size of the data returned by <a>GetRecords</a> varies
         * depending on the utilization of the shard. The maximum size of data that
         * <a>GetRecords</a> can return is 10 MB. If a call returns this amount of data,
         * subsequent calls made within the next 5 seconds throw
         * <code>ProvisionedThroughputExceededException</code>. If there is insufficient
         * provisioned throughput on the shard, subsequent calls made within the next 1
         * second throw <code>ProvisionedThroughputExceededException</code>.
         * <a>GetRecords</a> won't return any data when it throws an exception. For this
         * reason, we recommend that you wait one second between calls to
         * <a>GetRecords</a>; however, it's possible that the application will get
         * exceptions for longer than 1 second.</p> <p>To detect whether the application is
         * falling behind in processing, you can use the <code>MillisBehindLatest</code>
         * response attribute. You can also monitor the stream using CloudWatch metrics and
         * other mechanisms (see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring.html">Monitoring</a>
         * in the <i>Amazon Kinesis Streams Developer Guide</i>).</p> <p>Each Amazon
         * Kinesis record includes a value, <code>ApproximateArrivalTimestamp</code>, that
         * is set when a stream successfully receives and stores a record. This is commonly
         * referred to as a server-side time stamp, whereas a client-side time stamp is set
         * when a data producer creates or sends the record to a stream (a data producer is
         * any data source putting data records into a stream, for example with
         * <a>PutRecords</a>). The time stamp has millisecond precision. There are no
         * guarantees about the time stamp accuracy, or that the time stamp is always
         * increasing. For example, records in a shard or across a stream might have time
         * stamps that are out of order.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetRecords">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::GetRecordsOutcomeCallable GetRecordsCallable(const Model::GetRecordsRequest& request) const;

        /**
         * <p>Gets data records from a Kinesis stream's shard.</p> <p>Specify a shard
         * iterator using the <code>ShardIterator</code> parameter. The shard iterator
         * specifies the position in the shard from which you want to start reading data
         * records sequentially. If there are no records available in the portion of the
         * shard that the iterator points to, <a>GetRecords</a> returns an empty list. It
         * might take multiple calls to get to a portion of the shard that contains
         * records.</p> <p>You can scale by provisioning multiple shards per stream while
         * considering service limits (for more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>). Your
         * application should have one thread per shard, each reading continuously from its
         * stream. To read from a stream continually, call <a>GetRecords</a> in a loop. Use
         * <a>GetShardIterator</a> to get the shard iterator to specify in the first
         * <a>GetRecords</a> call. <a>GetRecords</a> returns a new shard iterator in
         * <code>NextShardIterator</code>. Specify the shard iterator returned in
         * <code>NextShardIterator</code> in subsequent calls to <a>GetRecords</a>. If the
         * shard has been closed, the shard iterator can't return more data and
         * <a>GetRecords</a> returns <code>null</code> in <code>NextShardIterator</code>.
         * You can terminate the loop when the shard is closed, or when the shard iterator
         * reaches the record with the sequence number or other attribute that marks it as
         * the last record to process.</p> <p>Each data record can be up to 1 MB in size,
         * and each shard can read up to 2 MB per second. You can ensure that your calls
         * don't exceed the maximum supported size or throughput by using the
         * <code>Limit</code> parameter to specify the maximum number of records that
         * <a>GetRecords</a> can return. Consider your average record size when determining
         * this limit.</p> <p>The size of the data returned by <a>GetRecords</a> varies
         * depending on the utilization of the shard. The maximum size of data that
         * <a>GetRecords</a> can return is 10 MB. If a call returns this amount of data,
         * subsequent calls made within the next 5 seconds throw
         * <code>ProvisionedThroughputExceededException</code>. If there is insufficient
         * provisioned throughput on the shard, subsequent calls made within the next 1
         * second throw <code>ProvisionedThroughputExceededException</code>.
         * <a>GetRecords</a> won't return any data when it throws an exception. For this
         * reason, we recommend that you wait one second between calls to
         * <a>GetRecords</a>; however, it's possible that the application will get
         * exceptions for longer than 1 second.</p> <p>To detect whether the application is
         * falling behind in processing, you can use the <code>MillisBehindLatest</code>
         * response attribute. You can also monitor the stream using CloudWatch metrics and
         * other mechanisms (see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/monitoring.html">Monitoring</a>
         * in the <i>Amazon Kinesis Streams Developer Guide</i>).</p> <p>Each Amazon
         * Kinesis record includes a value, <code>ApproximateArrivalTimestamp</code>, that
         * is set when a stream successfully receives and stores a record. This is commonly
         * referred to as a server-side time stamp, whereas a client-side time stamp is set
         * when a data producer creates or sends the record to a stream (a data producer is
         * any data source putting data records into a stream, for example with
         * <a>PutRecords</a>). The time stamp has millisecond precision. There are no
         * guarantees about the time stamp accuracy, or that the time stamp is always
         * increasing. For example, records in a shard or across a stream might have time
         * stamps that are out of order.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetRecords">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void GetRecordsAsync(const Model::GetRecordsRequest& request, const GetRecordsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Gets an Amazon Kinesis shard iterator. A shard iterator expires five minutes
         * after it is returned to the requester.</p> <p>A shard iterator specifies the
         * shard position from which to start reading data records sequentially. The
         * position is specified using the sequence number of a data record in a shard. A
         * sequence number is the identifier associated with every record ingested in the
         * stream, and is assigned when a record is put into the stream. Each stream has
         * one or more shards.</p> <p>You must specify the shard iterator type. For
         * example, you can set the <code>ShardIteratorType</code> parameter to read
         * exactly from the position denoted by a specific sequence number by using the
         * <code>AT_SEQUENCE_NUMBER</code> shard iterator type. Alternatively, the
         * parameter can read right after the sequence number by using the
         * <code>AFTER_SEQUENCE_NUMBER</code> shard iterator type, using sequence numbers
         * returned by earlier calls to <a>PutRecord</a>, <a>PutRecords</a>,
         * <a>GetRecords</a>, or <a>DescribeStream</a>. In the request, you can specify the
         * shard iterator type <code>AT_TIMESTAMP</code> to read records from an arbitrary
         * point in time, <code>TRIM_HORIZON</code> to cause <code>ShardIterator</code> to
         * point to the last untrimmed record in the shard in the system (the oldest data
         * record in the shard), or <code>LATEST</code> so that you always read the most
         * recent data in the shard. </p> <p>When you read repeatedly from a stream, use a
         * <a>GetShardIterator</a> request to get the first shard iterator for use in your
         * first <a>GetRecords</a> request and for subsequent reads use the shard iterator
         * returned by the <a>GetRecords</a> request in <code>NextShardIterator</code>. A
         * new shard iterator is returned by every <a>GetRecords</a> request in
         * <code>NextShardIterator</code>, which you use in the <code>ShardIterator</code>
         * parameter of the next <a>GetRecords</a> request. </p> <p>If a
         * <a>GetShardIterator</a> request is made too often, you receive a
         * <code>ProvisionedThroughputExceededException</code>. For more information about
         * throughput limits, see <a>GetRecords</a>, and <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>If the
         * shard is closed, <a>GetShardIterator</a> returns a valid iterator for the last
         * sequence number of the shard. A shard can be closed as a result of using
         * <a>SplitShard</a> or <a>MergeShards</a>.</p> <p> <a>GetShardIterator</a> has a
         * limit of 5 transactions per second per account per open shard.</p><p><h3>See
         * Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetShardIterator">AWS
         * API Reference</a></p>
         */
        virtual Model::GetShardIteratorOutcome GetShardIterator(const Model::GetShardIteratorRequest& request) const;

        /**
         * <p>Gets an Amazon Kinesis shard iterator. A shard iterator expires five minutes
         * after it is returned to the requester.</p> <p>A shard iterator specifies the
         * shard position from which to start reading data records sequentially. The
         * position is specified using the sequence number of a data record in a shard. A
         * sequence number is the identifier associated with every record ingested in the
         * stream, and is assigned when a record is put into the stream. Each stream has
         * one or more shards.</p> <p>You must specify the shard iterator type. For
         * example, you can set the <code>ShardIteratorType</code> parameter to read
         * exactly from the position denoted by a specific sequence number by using the
         * <code>AT_SEQUENCE_NUMBER</code> shard iterator type. Alternatively, the
         * parameter can read right after the sequence number by using the
         * <code>AFTER_SEQUENCE_NUMBER</code> shard iterator type, using sequence numbers
         * returned by earlier calls to <a>PutRecord</a>, <a>PutRecords</a>,
         * <a>GetRecords</a>, or <a>DescribeStream</a>. In the request, you can specify the
         * shard iterator type <code>AT_TIMESTAMP</code> to read records from an arbitrary
         * point in time, <code>TRIM_HORIZON</code> to cause <code>ShardIterator</code> to
         * point to the last untrimmed record in the shard in the system (the oldest data
         * record in the shard), or <code>LATEST</code> so that you always read the most
         * recent data in the shard. </p> <p>When you read repeatedly from a stream, use a
         * <a>GetShardIterator</a> request to get the first shard iterator for use in your
         * first <a>GetRecords</a> request and for subsequent reads use the shard iterator
         * returned by the <a>GetRecords</a> request in <code>NextShardIterator</code>. A
         * new shard iterator is returned by every <a>GetRecords</a> request in
         * <code>NextShardIterator</code>, which you use in the <code>ShardIterator</code>
         * parameter of the next <a>GetRecords</a> request. </p> <p>If a
         * <a>GetShardIterator</a> request is made too often, you receive a
         * <code>ProvisionedThroughputExceededException</code>. For more information about
         * throughput limits, see <a>GetRecords</a>, and <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>If the
         * shard is closed, <a>GetShardIterator</a> returns a valid iterator for the last
         * sequence number of the shard. A shard can be closed as a result of using
         * <a>SplitShard</a> or <a>MergeShards</a>.</p> <p> <a>GetShardIterator</a> has a
         * limit of 5 transactions per second per account per open shard.</p><p><h3>See
         * Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetShardIterator">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::GetShardIteratorOutcomeCallable GetShardIteratorCallable(const Model::GetShardIteratorRequest& request) const;

        /**
         * <p>Gets an Amazon Kinesis shard iterator. A shard iterator expires five minutes
         * after it is returned to the requester.</p> <p>A shard iterator specifies the
         * shard position from which to start reading data records sequentially. The
         * position is specified using the sequence number of a data record in a shard. A
         * sequence number is the identifier associated with every record ingested in the
         * stream, and is assigned when a record is put into the stream. Each stream has
         * one or more shards.</p> <p>You must specify the shard iterator type. For
         * example, you can set the <code>ShardIteratorType</code> parameter to read
         * exactly from the position denoted by a specific sequence number by using the
         * <code>AT_SEQUENCE_NUMBER</code> shard iterator type. Alternatively, the
         * parameter can read right after the sequence number by using the
         * <code>AFTER_SEQUENCE_NUMBER</code> shard iterator type, using sequence numbers
         * returned by earlier calls to <a>PutRecord</a>, <a>PutRecords</a>,
         * <a>GetRecords</a>, or <a>DescribeStream</a>. In the request, you can specify the
         * shard iterator type <code>AT_TIMESTAMP</code> to read records from an arbitrary
         * point in time, <code>TRIM_HORIZON</code> to cause <code>ShardIterator</code> to
         * point to the last untrimmed record in the shard in the system (the oldest data
         * record in the shard), or <code>LATEST</code> so that you always read the most
         * recent data in the shard. </p> <p>When you read repeatedly from a stream, use a
         * <a>GetShardIterator</a> request to get the first shard iterator for use in your
         * first <a>GetRecords</a> request and for subsequent reads use the shard iterator
         * returned by the <a>GetRecords</a> request in <code>NextShardIterator</code>. A
         * new shard iterator is returned by every <a>GetRecords</a> request in
         * <code>NextShardIterator</code>, which you use in the <code>ShardIterator</code>
         * parameter of the next <a>GetRecords</a> request. </p> <p>If a
         * <a>GetShardIterator</a> request is made too often, you receive a
         * <code>ProvisionedThroughputExceededException</code>. For more information about
         * throughput limits, see <a>GetRecords</a>, and <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>If the
         * shard is closed, <a>GetShardIterator</a> returns a valid iterator for the last
         * sequence number of the shard. A shard can be closed as a result of using
         * <a>SplitShard</a> or <a>MergeShards</a>.</p> <p> <a>GetShardIterator</a> has a
         * limit of 5 transactions per second per account per open shard.</p><p><h3>See
         * Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/GetShardIterator">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void GetShardIteratorAsync(const Model::GetShardIteratorRequest& request, const GetShardIteratorResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Increases the Amazon Kinesis stream's retention period, which is the length
         * of time data records are accessible after they are added to the stream. The
         * maximum value of a stream's retention period is 168 hours (7 days).</p> <p>If
         * you choose a longer stream retention period, this operation increases the time
         * period during which records that have not yet expired are accessible. However,
         * it does not make previous, expired data (older than the stream's previous
         * retention period) accessible after the operation has been called. For example,
         * if a stream's retention period is set to 24 hours and is increased to 168 hours,
         * any data that is older than 24 hours remains inaccessible to consumer
         * applications.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/IncreaseStreamRetentionPeriod">AWS
         * API Reference</a></p>
         */
        virtual Model::IncreaseStreamRetentionPeriodOutcome IncreaseStreamRetentionPeriod(const Model::IncreaseStreamRetentionPeriodRequest& request) const;

        /**
         * <p>Increases the Amazon Kinesis stream's retention period, which is the length
         * of time data records are accessible after they are added to the stream. The
         * maximum value of a stream's retention period is 168 hours (7 days).</p> <p>If
         * you choose a longer stream retention period, this operation increases the time
         * period during which records that have not yet expired are accessible. However,
         * it does not make previous, expired data (older than the stream's previous
         * retention period) accessible after the operation has been called. For example,
         * if a stream's retention period is set to 24 hours and is increased to 168 hours,
         * any data that is older than 24 hours remains inaccessible to consumer
         * applications.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/IncreaseStreamRetentionPeriod">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::IncreaseStreamRetentionPeriodOutcomeCallable IncreaseStreamRetentionPeriodCallable(const Model::IncreaseStreamRetentionPeriodRequest& request) const;

        /**
         * <p>Increases the Amazon Kinesis stream's retention period, which is the length
         * of time data records are accessible after they are added to the stream. The
         * maximum value of a stream's retention period is 168 hours (7 days).</p> <p>If
         * you choose a longer stream retention period, this operation increases the time
         * period during which records that have not yet expired are accessible. However,
         * it does not make previous, expired data (older than the stream's previous
         * retention period) accessible after the operation has been called. For example,
         * if a stream's retention period is set to 24 hours and is increased to 168 hours,
         * any data that is older than 24 hours remains inaccessible to consumer
         * applications.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/IncreaseStreamRetentionPeriod">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void IncreaseStreamRetentionPeriodAsync(const Model::IncreaseStreamRetentionPeriodRequest& request, const IncreaseStreamRetentionPeriodResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Lists your Kinesis streams.</p> <p>The number of streams may be too large to
         * return from a single call to <code>ListStreams</code>. You can limit the number
         * of returned streams using the <code>Limit</code> parameter. If you do not
         * specify a value for the <code>Limit</code> parameter, Kinesis Streams uses the
         * default limit, which is currently 10.</p> <p>You can detect if there are more
         * streams available to list by using the <code>HasMoreStreams</code> flag from the
         * returned output. If there are more streams available, you can request more
         * streams by using the name of the last stream returned by the
         * <code>ListStreams</code> request in the <code>ExclusiveStartStreamName</code>
         * parameter in a subsequent request to <code>ListStreams</code>. The group of
         * stream names returned by the subsequent request is then added to the list. You
         * can continue this process until all the stream names have been collected in the
         * list. </p> <p> <a>ListStreams</a> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListStreams">AWS
         * API Reference</a></p>
         */
        virtual Model::ListStreamsOutcome ListStreams(const Model::ListStreamsRequest& request) const;

        /**
         * <p>Lists your Kinesis streams.</p> <p>The number of streams may be too large to
         * return from a single call to <code>ListStreams</code>. You can limit the number
         * of returned streams using the <code>Limit</code> parameter. If you do not
         * specify a value for the <code>Limit</code> parameter, Kinesis Streams uses the
         * default limit, which is currently 10.</p> <p>You can detect if there are more
         * streams available to list by using the <code>HasMoreStreams</code> flag from the
         * returned output. If there are more streams available, you can request more
         * streams by using the name of the last stream returned by the
         * <code>ListStreams</code> request in the <code>ExclusiveStartStreamName</code>
         * parameter in a subsequent request to <code>ListStreams</code>. The group of
         * stream names returned by the subsequent request is then added to the list. You
         * can continue this process until all the stream names have been collected in the
         * list. </p> <p> <a>ListStreams</a> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListStreams">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::ListStreamsOutcomeCallable ListStreamsCallable(const Model::ListStreamsRequest& request) const;

        /**
         * <p>Lists your Kinesis streams.</p> <p>The number of streams may be too large to
         * return from a single call to <code>ListStreams</code>. You can limit the number
         * of returned streams using the <code>Limit</code> parameter. If you do not
         * specify a value for the <code>Limit</code> parameter, Kinesis Streams uses the
         * default limit, which is currently 10.</p> <p>You can detect if there are more
         * streams available to list by using the <code>HasMoreStreams</code> flag from the
         * returned output. If there are more streams available, you can request more
         * streams by using the name of the last stream returned by the
         * <code>ListStreams</code> request in the <code>ExclusiveStartStreamName</code>
         * parameter in a subsequent request to <code>ListStreams</code>. The group of
         * stream names returned by the subsequent request is then added to the list. You
         * can continue this process until all the stream names have been collected in the
         * list. </p> <p> <a>ListStreams</a> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListStreams">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void ListStreamsAsync(const Model::ListStreamsRequest& request, const ListStreamsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Lists the tags for the specified Kinesis stream.</p><p><h3>See Also:</h3>  
         * <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListTagsForStream">AWS
         * API Reference</a></p>
         */
        virtual Model::ListTagsForStreamOutcome ListTagsForStream(const Model::ListTagsForStreamRequest& request) const;

        /**
         * <p>Lists the tags for the specified Kinesis stream.</p><p><h3>See Also:</h3>  
         * <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListTagsForStream">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::ListTagsForStreamOutcomeCallable ListTagsForStreamCallable(const Model::ListTagsForStreamRequest& request) const;

        /**
         * <p>Lists the tags for the specified Kinesis stream.</p><p><h3>See Also:</h3>  
         * <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/ListTagsForStream">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void ListTagsForStreamAsync(const Model::ListTagsForStreamRequest& request, const ListTagsForStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Merges two adjacent shards in a Kinesis stream and combines them into a
         * single shard to reduce the stream's capacity to ingest and transport data. Two
         * shards are considered adjacent if the union of the hash key ranges for the two
         * shards form a contiguous set with no gaps. For example, if you have two shards,
         * one with a hash key range of 276...381 and the other with a hash key range of
         * 382...454, then you could merge these two shards into a single shard that would
         * have a hash key range of 276...454. After the merge, the single child shard
         * receives data for all hash key values covered by the two parent shards.</p> <p>
         * <code>MergeShards</code> is called when there is a need to reduce the overall
         * capacity of a stream because of excess capacity that is not being used. You must
         * specify the shard to be merged and the adjacent shard for a stream. For more
         * information about merging shards, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-resharding-merge.html">Merge
         * Two Shards</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>If
         * the stream is in the <code>ACTIVE</code> state, you can call
         * <code>MergeShards</code>. If a stream is in the <code>CREATING</code>,
         * <code>UPDATING</code>, or <code>DELETING</code> state, <code>MergeShards</code>
         * returns a <code>ResourceInUseException</code>. If the specified stream does not
         * exist, <code>MergeShards</code> returns a
         * <code>ResourceNotFoundException</code>. </p> <p>You can use
         * <a>DescribeStream</a> to check the state of the stream, which is returned in
         * <code>StreamStatus</code>.</p> <p> <code>MergeShards</code> is an asynchronous
         * operation. Upon receiving a <code>MergeShards</code> request, Amazon Kinesis
         * immediately returns a response and sets the <code>StreamStatus</code> to
         * <code>UPDATING</code>. After the operation is completed, Amazon Kinesis sets the
         * <code>StreamStatus</code> to <code>ACTIVE</code>. Read and write operations
         * continue to work while the stream is in the <code>UPDATING</code> state. </p>
         * <p>You use <a>DescribeStream</a> to determine the shard IDs that are specified
         * in the <code>MergeShards</code> request. </p> <p>If you try to operate on too
         * many streams in parallel using <a>CreateStream</a>, <a>DeleteStream</a>,
         * <code>MergeShards</code> or <a>SplitShard</a>, you will receive a
         * <code>LimitExceededException</code>. </p> <p> <code>MergeShards</code> has a
         * limit of 5 transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/MergeShards">AWS
         * API Reference</a></p>
         */
        virtual Model::MergeShardsOutcome MergeShards(const Model::MergeShardsRequest& request) const;

        /**
         * <p>Merges two adjacent shards in a Kinesis stream and combines them into a
         * single shard to reduce the stream's capacity to ingest and transport data. Two
         * shards are considered adjacent if the union of the hash key ranges for the two
         * shards form a contiguous set with no gaps. For example, if you have two shards,
         * one with a hash key range of 276...381 and the other with a hash key range of
         * 382...454, then you could merge these two shards into a single shard that would
         * have a hash key range of 276...454. After the merge, the single child shard
         * receives data for all hash key values covered by the two parent shards.</p> <p>
         * <code>MergeShards</code> is called when there is a need to reduce the overall
         * capacity of a stream because of excess capacity that is not being used. You must
         * specify the shard to be merged and the adjacent shard for a stream. For more
         * information about merging shards, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-resharding-merge.html">Merge
         * Two Shards</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>If
         * the stream is in the <code>ACTIVE</code> state, you can call
         * <code>MergeShards</code>. If a stream is in the <code>CREATING</code>,
         * <code>UPDATING</code>, or <code>DELETING</code> state, <code>MergeShards</code>
         * returns a <code>ResourceInUseException</code>. If the specified stream does not
         * exist, <code>MergeShards</code> returns a
         * <code>ResourceNotFoundException</code>. </p> <p>You can use
         * <a>DescribeStream</a> to check the state of the stream, which is returned in
         * <code>StreamStatus</code>.</p> <p> <code>MergeShards</code> is an asynchronous
         * operation. Upon receiving a <code>MergeShards</code> request, Amazon Kinesis
         * immediately returns a response and sets the <code>StreamStatus</code> to
         * <code>UPDATING</code>. After the operation is completed, Amazon Kinesis sets the
         * <code>StreamStatus</code> to <code>ACTIVE</code>. Read and write operations
         * continue to work while the stream is in the <code>UPDATING</code> state. </p>
         * <p>You use <a>DescribeStream</a> to determine the shard IDs that are specified
         * in the <code>MergeShards</code> request. </p> <p>If you try to operate on too
         * many streams in parallel using <a>CreateStream</a>, <a>DeleteStream</a>,
         * <code>MergeShards</code> or <a>SplitShard</a>, you will receive a
         * <code>LimitExceededException</code>. </p> <p> <code>MergeShards</code> has a
         * limit of 5 transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/MergeShards">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::MergeShardsOutcomeCallable MergeShardsCallable(const Model::MergeShardsRequest& request) const;

        /**
         * <p>Merges two adjacent shards in a Kinesis stream and combines them into a
         * single shard to reduce the stream's capacity to ingest and transport data. Two
         * shards are considered adjacent if the union of the hash key ranges for the two
         * shards form a contiguous set with no gaps. For example, if you have two shards,
         * one with a hash key range of 276...381 and the other with a hash key range of
         * 382...454, then you could merge these two shards into a single shard that would
         * have a hash key range of 276...454. After the merge, the single child shard
         * receives data for all hash key values covered by the two parent shards.</p> <p>
         * <code>MergeShards</code> is called when there is a need to reduce the overall
         * capacity of a stream because of excess capacity that is not being used. You must
         * specify the shard to be merged and the adjacent shard for a stream. For more
         * information about merging shards, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-resharding-merge.html">Merge
         * Two Shards</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>If
         * the stream is in the <code>ACTIVE</code> state, you can call
         * <code>MergeShards</code>. If a stream is in the <code>CREATING</code>,
         * <code>UPDATING</code>, or <code>DELETING</code> state, <code>MergeShards</code>
         * returns a <code>ResourceInUseException</code>. If the specified stream does not
         * exist, <code>MergeShards</code> returns a
         * <code>ResourceNotFoundException</code>. </p> <p>You can use
         * <a>DescribeStream</a> to check the state of the stream, which is returned in
         * <code>StreamStatus</code>.</p> <p> <code>MergeShards</code> is an asynchronous
         * operation. Upon receiving a <code>MergeShards</code> request, Amazon Kinesis
         * immediately returns a response and sets the <code>StreamStatus</code> to
         * <code>UPDATING</code>. After the operation is completed, Amazon Kinesis sets the
         * <code>StreamStatus</code> to <code>ACTIVE</code>. Read and write operations
         * continue to work while the stream is in the <code>UPDATING</code> state. </p>
         * <p>You use <a>DescribeStream</a> to determine the shard IDs that are specified
         * in the <code>MergeShards</code> request. </p> <p>If you try to operate on too
         * many streams in parallel using <a>CreateStream</a>, <a>DeleteStream</a>,
         * <code>MergeShards</code> or <a>SplitShard</a>, you will receive a
         * <code>LimitExceededException</code>. </p> <p> <code>MergeShards</code> has a
         * limit of 5 transactions per second per account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/MergeShards">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void MergeShardsAsync(const Model::MergeShardsRequest& request, const MergeShardsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Writes a single data record into an Amazon Kinesis stream. Call
         * <code>PutRecord</code> to send data into the stream for real-time ingestion and
         * subsequent processing, one record at a time. Each shard can support writes up to
         * 1,000 records per second, up to a maximum data write total of 1 MB per
         * second.</p> <p>You must specify the name of the stream that captures, stores,
         * and transports the data; a partition key; and the data blob itself.</p> <p>The
         * data blob can be any type of data; for example, a segment from a log file,
         * geographic/location data, website clickstream data, and so on.</p> <p>The
         * partition key is used by Kinesis Streams to distribute data across shards.
         * Kinesis Streams segregates the data records that belong to a stream into
         * multiple shards, using the partition key associated with each data record to
         * determine the shard to which a given data record belongs.</p> <p>Partition keys
         * are Unicode strings, with a maximum length limit of 256 characters for each key.
         * An MD5 hash function is used to map partition keys to 128-bit integer values and
         * to map associated data records to shards using the hash key ranges of the
         * shards. You can override hashing the partition key to determine the shard by
         * explicitly specifying a hash value using the <code>ExplicitHashKey</code>
         * parameter. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p> <code>PutRecord</code> returns the shard ID of where the data record was
         * placed and the sequence number that was assigned to the data record.</p>
         * <p>Sequence numbers increase over time and are specific to a shard within a
         * stream, not across all shards within a stream. To guarantee strictly increasing
         * ordering, write serially to a shard and use the
         * <code>SequenceNumberForOrdering</code> parameter. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p>If a <code>PutRecord</code> request cannot be processed because of
         * insufficient provisioned throughput on the shard involved in the request,
         * <code>PutRecord</code> throws
         * <code>ProvisionedThroughputExceededException</code>. </p> <p>By default, data
         * records are accessible for 24 hours from the time that they are added to a
         * stream. You can use <a>IncreaseStreamRetentionPeriod</a> or
         * <a>DecreaseStreamRetentionPeriod</a> to modify this retention
         * period.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecord">AWS
         * API Reference</a></p>
         */
        virtual Model::PutRecordOutcome PutRecord(const Model::PutRecordRequest& request) const;

        /**
         * <p>Writes a single data record into an Amazon Kinesis stream. Call
         * <code>PutRecord</code> to send data into the stream for real-time ingestion and
         * subsequent processing, one record at a time. Each shard can support writes up to
         * 1,000 records per second, up to a maximum data write total of 1 MB per
         * second.</p> <p>You must specify the name of the stream that captures, stores,
         * and transports the data; a partition key; and the data blob itself.</p> <p>The
         * data blob can be any type of data; for example, a segment from a log file,
         * geographic/location data, website clickstream data, and so on.</p> <p>The
         * partition key is used by Kinesis Streams to distribute data across shards.
         * Kinesis Streams segregates the data records that belong to a stream into
         * multiple shards, using the partition key associated with each data record to
         * determine the shard to which a given data record belongs.</p> <p>Partition keys
         * are Unicode strings, with a maximum length limit of 256 characters for each key.
         * An MD5 hash function is used to map partition keys to 128-bit integer values and
         * to map associated data records to shards using the hash key ranges of the
         * shards. You can override hashing the partition key to determine the shard by
         * explicitly specifying a hash value using the <code>ExplicitHashKey</code>
         * parameter. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p> <code>PutRecord</code> returns the shard ID of where the data record was
         * placed and the sequence number that was assigned to the data record.</p>
         * <p>Sequence numbers increase over time and are specific to a shard within a
         * stream, not across all shards within a stream. To guarantee strictly increasing
         * ordering, write serially to a shard and use the
         * <code>SequenceNumberForOrdering</code> parameter. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p>If a <code>PutRecord</code> request cannot be processed because of
         * insufficient provisioned throughput on the shard involved in the request,
         * <code>PutRecord</code> throws
         * <code>ProvisionedThroughputExceededException</code>. </p> <p>By default, data
         * records are accessible for 24 hours from the time that they are added to a
         * stream. You can use <a>IncreaseStreamRetentionPeriod</a> or
         * <a>DecreaseStreamRetentionPeriod</a> to modify this retention
         * period.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecord">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::PutRecordOutcomeCallable PutRecordCallable(const Model::PutRecordRequest& request) const;

        /**
         * <p>Writes a single data record into an Amazon Kinesis stream. Call
         * <code>PutRecord</code> to send data into the stream for real-time ingestion and
         * subsequent processing, one record at a time. Each shard can support writes up to
         * 1,000 records per second, up to a maximum data write total of 1 MB per
         * second.</p> <p>You must specify the name of the stream that captures, stores,
         * and transports the data; a partition key; and the data blob itself.</p> <p>The
         * data blob can be any type of data; for example, a segment from a log file,
         * geographic/location data, website clickstream data, and so on.</p> <p>The
         * partition key is used by Kinesis Streams to distribute data across shards.
         * Kinesis Streams segregates the data records that belong to a stream into
         * multiple shards, using the partition key associated with each data record to
         * determine the shard to which a given data record belongs.</p> <p>Partition keys
         * are Unicode strings, with a maximum length limit of 256 characters for each key.
         * An MD5 hash function is used to map partition keys to 128-bit integer values and
         * to map associated data records to shards using the hash key ranges of the
         * shards. You can override hashing the partition key to determine the shard by
         * explicitly specifying a hash value using the <code>ExplicitHashKey</code>
         * parameter. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p> <code>PutRecord</code> returns the shard ID of where the data record was
         * placed and the sequence number that was assigned to the data record.</p>
         * <p>Sequence numbers increase over time and are specific to a shard within a
         * stream, not across all shards within a stream. To guarantee strictly increasing
         * ordering, write serially to a shard and use the
         * <code>SequenceNumberForOrdering</code> parameter. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p>If a <code>PutRecord</code> request cannot be processed because of
         * insufficient provisioned throughput on the shard involved in the request,
         * <code>PutRecord</code> throws
         * <code>ProvisionedThroughputExceededException</code>. </p> <p>By default, data
         * records are accessible for 24 hours from the time that they are added to a
         * stream. You can use <a>IncreaseStreamRetentionPeriod</a> or
         * <a>DecreaseStreamRetentionPeriod</a> to modify this retention
         * period.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecord">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void PutRecordAsync(const Model::PutRecordRequest& request, const PutRecordResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Writes multiple data records into a Kinesis stream in a single call (also
         * referred to as a <code>PutRecords</code> request). Use this operation to send
         * data into the stream for data ingestion and processing. </p> <p>Each
         * <code>PutRecords</code> request can support up to 500 records. Each record in
         * the request can be as large as 1 MB, up to a limit of 5 MB for the entire
         * request, including partition keys. Each shard can support writes up to 1,000
         * records per second, up to a maximum data write total of 1 MB per second.</p>
         * <p>You must specify the name of the stream that captures, stores, and transports
         * the data; and an array of request <code>Records</code>, with each record in the
         * array requiring a partition key and data blob. The record size limit applies to
         * the total size of the partition key and data blob.</p> <p>The data blob can be
         * any type of data; for example, a segment from a log file, geographic/location
         * data, website clickstream data, and so on.</p> <p>The partition key is used by
         * Kinesis Streams as input to a hash function that maps the partition key and
         * associated data to a specific shard. An MD5 hash function is used to map
         * partition keys to 128-bit integer values and to map associated data records to
         * shards. As a result of this hashing mechanism, all data records with the same
         * partition key map to the same shard within the stream. For more information, see
         * <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p>Each record in the <code>Records</code> array may include an optional
         * parameter, <code>ExplicitHashKey</code>, which overrides the partition key to
         * shard mapping. This parameter allows a data producer to determine explicitly the
         * shard where the record is stored. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-putrecords">Adding
         * Multiple Records with PutRecords</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>The <code>PutRecords</code> response includes an array of
         * response <code>Records</code>. Each record in the response array directly
         * correlates with a record in the request array using natural ordering, from the
         * top to the bottom of the request and response. The response <code>Records</code>
         * array always includes the same number of records as the request array.</p>
         * <p>The response <code>Records</code> array includes both successfully and
         * unsuccessfully processed records. Amazon Kinesis attempts to process all records
         * in each <code>PutRecords</code> request. A single record failure does not stop
         * the processing of subsequent records.</p> <p>A successfully processed record
         * includes <code>ShardId</code> and <code>SequenceNumber</code> values. The
         * <code>ShardId</code> parameter identifies the shard in the stream where the
         * record is stored. The <code>SequenceNumber</code> parameter is an identifier
         * assigned to the put record, unique to all records in the stream.</p> <p>An
         * unsuccessfully processed record includes <code>ErrorCode</code> and
         * <code>ErrorMessage</code> values. <code>ErrorCode</code> reflects the type of
         * error and can be one of the following values:
         * <code>ProvisionedThroughputExceededException</code> or
         * <code>InternalFailure</code>. <code>ErrorMessage</code> provides more detailed
         * information about the <code>ProvisionedThroughputExceededException</code>
         * exception including the account ID, stream name, and shard ID of the record that
         * was throttled. For more information about partially successful responses, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-add-data-to-stream.html#kinesis-using-sdk-java-putrecords">Adding
         * Multiple Records with PutRecords</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>By default, data records are accessible for 24 hours from the
         * time that they are added to a stream. You can use
         * <a>IncreaseStreamRetentionPeriod</a> or <a>DecreaseStreamRetentionPeriod</a> to
         * modify this retention period.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecords">AWS
         * API Reference</a></p>
         */
        virtual Model::PutRecordsOutcome PutRecords(const Model::PutRecordsRequest& request) const;

        /**
         * <p>Writes multiple data records into a Kinesis stream in a single call (also
         * referred to as a <code>PutRecords</code> request). Use this operation to send
         * data into the stream for data ingestion and processing. </p> <p>Each
         * <code>PutRecords</code> request can support up to 500 records. Each record in
         * the request can be as large as 1 MB, up to a limit of 5 MB for the entire
         * request, including partition keys. Each shard can support writes up to 1,000
         * records per second, up to a maximum data write total of 1 MB per second.</p>
         * <p>You must specify the name of the stream that captures, stores, and transports
         * the data; and an array of request <code>Records</code>, with each record in the
         * array requiring a partition key and data blob. The record size limit applies to
         * the total size of the partition key and data blob.</p> <p>The data blob can be
         * any type of data; for example, a segment from a log file, geographic/location
         * data, website clickstream data, and so on.</p> <p>The partition key is used by
         * Kinesis Streams as input to a hash function that maps the partition key and
         * associated data to a specific shard. An MD5 hash function is used to map
         * partition keys to 128-bit integer values and to map associated data records to
         * shards. As a result of this hashing mechanism, all data records with the same
         * partition key map to the same shard within the stream. For more information, see
         * <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p>Each record in the <code>Records</code> array may include an optional
         * parameter, <code>ExplicitHashKey</code>, which overrides the partition key to
         * shard mapping. This parameter allows a data producer to determine explicitly the
         * shard where the record is stored. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-putrecords">Adding
         * Multiple Records with PutRecords</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>The <code>PutRecords</code> response includes an array of
         * response <code>Records</code>. Each record in the response array directly
         * correlates with a record in the request array using natural ordering, from the
         * top to the bottom of the request and response. The response <code>Records</code>
         * array always includes the same number of records as the request array.</p>
         * <p>The response <code>Records</code> array includes both successfully and
         * unsuccessfully processed records. Amazon Kinesis attempts to process all records
         * in each <code>PutRecords</code> request. A single record failure does not stop
         * the processing of subsequent records.</p> <p>A successfully processed record
         * includes <code>ShardId</code> and <code>SequenceNumber</code> values. The
         * <code>ShardId</code> parameter identifies the shard in the stream where the
         * record is stored. The <code>SequenceNumber</code> parameter is an identifier
         * assigned to the put record, unique to all records in the stream.</p> <p>An
         * unsuccessfully processed record includes <code>ErrorCode</code> and
         * <code>ErrorMessage</code> values. <code>ErrorCode</code> reflects the type of
         * error and can be one of the following values:
         * <code>ProvisionedThroughputExceededException</code> or
         * <code>InternalFailure</code>. <code>ErrorMessage</code> provides more detailed
         * information about the <code>ProvisionedThroughputExceededException</code>
         * exception including the account ID, stream name, and shard ID of the record that
         * was throttled. For more information about partially successful responses, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-add-data-to-stream.html#kinesis-using-sdk-java-putrecords">Adding
         * Multiple Records with PutRecords</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>By default, data records are accessible for 24 hours from the
         * time that they are added to a stream. You can use
         * <a>IncreaseStreamRetentionPeriod</a> or <a>DecreaseStreamRetentionPeriod</a> to
         * modify this retention period.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecords">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::PutRecordsOutcomeCallable PutRecordsCallable(const Model::PutRecordsRequest& request) const;

        /**
         * <p>Writes multiple data records into a Kinesis stream in a single call (also
         * referred to as a <code>PutRecords</code> request). Use this operation to send
         * data into the stream for data ingestion and processing. </p> <p>Each
         * <code>PutRecords</code> request can support up to 500 records. Each record in
         * the request can be as large as 1 MB, up to a limit of 5 MB for the entire
         * request, including partition keys. Each shard can support writes up to 1,000
         * records per second, up to a maximum data write total of 1 MB per second.</p>
         * <p>You must specify the name of the stream that captures, stores, and transports
         * the data; and an array of request <code>Records</code>, with each record in the
         * array requiring a partition key and data blob. The record size limit applies to
         * the total size of the partition key and data blob.</p> <p>The data blob can be
         * any type of data; for example, a segment from a log file, geographic/location
         * data, website clickstream data, and so on.</p> <p>The partition key is used by
         * Kinesis Streams as input to a hash function that maps the partition key and
         * associated data to a specific shard. An MD5 hash function is used to map
         * partition keys to 128-bit integer values and to map associated data records to
         * shards. As a result of this hashing mechanism, all data records with the same
         * partition key map to the same shard within the stream. For more information, see
         * <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-add-data-to-stream">Adding
         * Data to a Stream</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p>
         * <p>Each record in the <code>Records</code> array may include an optional
         * parameter, <code>ExplicitHashKey</code>, which overrides the partition key to
         * shard mapping. This parameter allows a data producer to determine explicitly the
         * shard where the record is stored. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-sdk.html#kinesis-using-sdk-java-putrecords">Adding
         * Multiple Records with PutRecords</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>The <code>PutRecords</code> response includes an array of
         * response <code>Records</code>. Each record in the response array directly
         * correlates with a record in the request array using natural ordering, from the
         * top to the bottom of the request and response. The response <code>Records</code>
         * array always includes the same number of records as the request array.</p>
         * <p>The response <code>Records</code> array includes both successfully and
         * unsuccessfully processed records. Amazon Kinesis attempts to process all records
         * in each <code>PutRecords</code> request. A single record failure does not stop
         * the processing of subsequent records.</p> <p>A successfully processed record
         * includes <code>ShardId</code> and <code>SequenceNumber</code> values. The
         * <code>ShardId</code> parameter identifies the shard in the stream where the
         * record is stored. The <code>SequenceNumber</code> parameter is an identifier
         * assigned to the put record, unique to all records in the stream.</p> <p>An
         * unsuccessfully processed record includes <code>ErrorCode</code> and
         * <code>ErrorMessage</code> values. <code>ErrorCode</code> reflects the type of
         * error and can be one of the following values:
         * <code>ProvisionedThroughputExceededException</code> or
         * <code>InternalFailure</code>. <code>ErrorMessage</code> provides more detailed
         * information about the <code>ProvisionedThroughputExceededException</code>
         * exception including the account ID, stream name, and shard ID of the record that
         * was throttled. For more information about partially successful responses, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-add-data-to-stream.html#kinesis-using-sdk-java-putrecords">Adding
         * Multiple Records with PutRecords</a> in the <i>Amazon Kinesis Streams Developer
         * Guide</i>.</p> <p>By default, data records are accessible for 24 hours from the
         * time that they are added to a stream. You can use
         * <a>IncreaseStreamRetentionPeriod</a> or <a>DecreaseStreamRetentionPeriod</a> to
         * modify this retention period.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecords">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void PutRecordsAsync(const Model::PutRecordsRequest& request, const PutRecordsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Removes tags from the specified Kinesis stream. Removed tags are deleted and
         * cannot be recovered after this operation successfully completes.</p> <p>If you
         * specify a tag that does not exist, it is ignored.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/RemoveTagsFromStream">AWS
         * API Reference</a></p>
         */
        virtual Model::RemoveTagsFromStreamOutcome RemoveTagsFromStream(const Model::RemoveTagsFromStreamRequest& request) const;

        /**
         * <p>Removes tags from the specified Kinesis stream. Removed tags are deleted and
         * cannot be recovered after this operation successfully completes.</p> <p>If you
         * specify a tag that does not exist, it is ignored.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/RemoveTagsFromStream">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::RemoveTagsFromStreamOutcomeCallable RemoveTagsFromStreamCallable(const Model::RemoveTagsFromStreamRequest& request) const;

        /**
         * <p>Removes tags from the specified Kinesis stream. Removed tags are deleted and
         * cannot be recovered after this operation successfully completes.</p> <p>If you
         * specify a tag that does not exist, it is ignored.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/RemoveTagsFromStream">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void RemoveTagsFromStreamAsync(const Model::RemoveTagsFromStreamRequest& request, const RemoveTagsFromStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Splits a shard into two new shards in the Kinesis stream, to increase the
         * stream's capacity to ingest and transport data. <code>SplitShard</code> is
         * called when there is a need to increase the overall capacity of a stream because
         * of an expected increase in the volume of data records being ingested. </p>
         * <p>You can also use <code>SplitShard</code> when a shard appears to be
         * approaching its maximum utilization; for example, the producers sending data
         * into the specific shard are suddenly sending more than previously anticipated.
         * You can also call <code>SplitShard</code> to increase stream capacity, so that
         * more Kinesis Streams applications can simultaneously read data from the stream
         * for real-time processing. </p> <p>You must specify the shard to be split and the
         * new hash key, which is the position in the shard where the shard gets split in
         * two. In many cases, the new hash key might be the average of the beginning and
         * ending hash key, but it can be any hash key value in the range being mapped into
         * the shard. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-resharding-split.html">Split
         * a Shard</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>You can
         * use <a>DescribeStream</a> to determine the shard ID and hash key values for the
         * <code>ShardToSplit</code> and <code>NewStartingHashKey</code> parameters that
         * are specified in the <code>SplitShard</code> request.</p> <p>
         * <code>SplitShard</code> is an asynchronous operation. Upon receiving a
         * <code>SplitShard</code> request, Kinesis Streams immediately returns a response
         * and sets the stream status to <code>UPDATING</code>. After the operation is
         * completed, Kinesis Streams sets the stream status to <code>ACTIVE</code>. Read
         * and write operations continue to work while the stream is in the
         * <code>UPDATING</code> state. </p> <p>You can use <code>DescribeStream</code> to
         * check the status of the stream, which is returned in <code>StreamStatus</code>.
         * If the stream is in the <code>ACTIVE</code> state, you can call
         * <code>SplitShard</code>. If a stream is in <code>CREATING</code> or
         * <code>UPDATING</code> or <code>DELETING</code> states,
         * <code>DescribeStream</code> returns a <code>ResourceInUseException</code>.</p>
         * <p>If the specified stream does not exist, <code>DescribeStream</code> returns a
         * <code>ResourceNotFoundException</code>. If you try to create more shards than
         * are authorized for your account, you receive a
         * <code>LimitExceededException</code>. </p> <p>For the default shard limit for an
         * AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase
         * this limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p> <p>If you try to operate on too many streams simultaneously
         * using <a>CreateStream</a>, <a>DeleteStream</a>, <a>MergeShards</a>, and/or
         * <a>SplitShard</a>, you receive a <code>LimitExceededException</code>. </p> <p>
         * <code>SplitShard</code> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/SplitShard">AWS
         * API Reference</a></p>
         */
        virtual Model::SplitShardOutcome SplitShard(const Model::SplitShardRequest& request) const;

        /**
         * <p>Splits a shard into two new shards in the Kinesis stream, to increase the
         * stream's capacity to ingest and transport data. <code>SplitShard</code> is
         * called when there is a need to increase the overall capacity of a stream because
         * of an expected increase in the volume of data records being ingested. </p>
         * <p>You can also use <code>SplitShard</code> when a shard appears to be
         * approaching its maximum utilization; for example, the producers sending data
         * into the specific shard are suddenly sending more than previously anticipated.
         * You can also call <code>SplitShard</code> to increase stream capacity, so that
         * more Kinesis Streams applications can simultaneously read data from the stream
         * for real-time processing. </p> <p>You must specify the shard to be split and the
         * new hash key, which is the position in the shard where the shard gets split in
         * two. In many cases, the new hash key might be the average of the beginning and
         * ending hash key, but it can be any hash key value in the range being mapped into
         * the shard. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-resharding-split.html">Split
         * a Shard</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>You can
         * use <a>DescribeStream</a> to determine the shard ID and hash key values for the
         * <code>ShardToSplit</code> and <code>NewStartingHashKey</code> parameters that
         * are specified in the <code>SplitShard</code> request.</p> <p>
         * <code>SplitShard</code> is an asynchronous operation. Upon receiving a
         * <code>SplitShard</code> request, Kinesis Streams immediately returns a response
         * and sets the stream status to <code>UPDATING</code>. After the operation is
         * completed, Kinesis Streams sets the stream status to <code>ACTIVE</code>. Read
         * and write operations continue to work while the stream is in the
         * <code>UPDATING</code> state. </p> <p>You can use <code>DescribeStream</code> to
         * check the status of the stream, which is returned in <code>StreamStatus</code>.
         * If the stream is in the <code>ACTIVE</code> state, you can call
         * <code>SplitShard</code>. If a stream is in <code>CREATING</code> or
         * <code>UPDATING</code> or <code>DELETING</code> states,
         * <code>DescribeStream</code> returns a <code>ResourceInUseException</code>.</p>
         * <p>If the specified stream does not exist, <code>DescribeStream</code> returns a
         * <code>ResourceNotFoundException</code>. If you try to create more shards than
         * are authorized for your account, you receive a
         * <code>LimitExceededException</code>. </p> <p>For the default shard limit for an
         * AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase
         * this limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p> <p>If you try to operate on too many streams simultaneously
         * using <a>CreateStream</a>, <a>DeleteStream</a>, <a>MergeShards</a>, and/or
         * <a>SplitShard</a>, you receive a <code>LimitExceededException</code>. </p> <p>
         * <code>SplitShard</code> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/SplitShard">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::SplitShardOutcomeCallable SplitShardCallable(const Model::SplitShardRequest& request) const;

        /**
         * <p>Splits a shard into two new shards in the Kinesis stream, to increase the
         * stream's capacity to ingest and transport data. <code>SplitShard</code> is
         * called when there is a need to increase the overall capacity of a stream because
         * of an expected increase in the volume of data records being ingested. </p>
         * <p>You can also use <code>SplitShard</code> when a shard appears to be
         * approaching its maximum utilization; for example, the producers sending data
         * into the specific shard are suddenly sending more than previously anticipated.
         * You can also call <code>SplitShard</code> to increase stream capacity, so that
         * more Kinesis Streams applications can simultaneously read data from the stream
         * for real-time processing. </p> <p>You must specify the shard to be split and the
         * new hash key, which is the position in the shard where the shard gets split in
         * two. In many cases, the new hash key might be the average of the beginning and
         * ending hash key, but it can be any hash key value in the range being mapped into
         * the shard. For more information, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-using-sdk-java-resharding-split.html">Split
         * a Shard</a> in the <i>Amazon Kinesis Streams Developer Guide</i>.</p> <p>You can
         * use <a>DescribeStream</a> to determine the shard ID and hash key values for the
         * <code>ShardToSplit</code> and <code>NewStartingHashKey</code> parameters that
         * are specified in the <code>SplitShard</code> request.</p> <p>
         * <code>SplitShard</code> is an asynchronous operation. Upon receiving a
         * <code>SplitShard</code> request, Kinesis Streams immediately returns a response
         * and sets the stream status to <code>UPDATING</code>. After the operation is
         * completed, Kinesis Streams sets the stream status to <code>ACTIVE</code>. Read
         * and write operations continue to work while the stream is in the
         * <code>UPDATING</code> state. </p> <p>You can use <code>DescribeStream</code> to
         * check the status of the stream, which is returned in <code>StreamStatus</code>.
         * If the stream is in the <code>ACTIVE</code> state, you can call
         * <code>SplitShard</code>. If a stream is in <code>CREATING</code> or
         * <code>UPDATING</code> or <code>DELETING</code> states,
         * <code>DescribeStream</code> returns a <code>ResourceInUseException</code>.</p>
         * <p>If the specified stream does not exist, <code>DescribeStream</code> returns a
         * <code>ResourceNotFoundException</code>. If you try to create more shards than
         * are authorized for your account, you receive a
         * <code>LimitExceededException</code>. </p> <p>For the default shard limit for an
         * AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase
         * this limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p> <p>If you try to operate on too many streams simultaneously
         * using <a>CreateStream</a>, <a>DeleteStream</a>, <a>MergeShards</a>, and/or
         * <a>SplitShard</a>, you receive a <code>LimitExceededException</code>. </p> <p>
         * <code>SplitShard</code> has a limit of 5 transactions per second per
         * account.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/SplitShard">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void SplitShardAsync(const Model::SplitShardRequest& request, const SplitShardResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Enables or updates server-side encryption using an AWS KMS key for a
         * specified stream. </p> <p>Starting encryption is an asynchronous operation. Upon
         * receiving the request, Kinesis Streams returns immediately and sets the status
         * of the stream to <code>UPDATING</code>. After the update is complete, Kinesis
         * Streams sets the status of the stream back to <code>ACTIVE</code>. Updating or
         * applying encryption normally takes a few seconds to complete, but it can take
         * minutes. You can continue to read and write data to your stream while its status
         * is <code>UPDATING</code>. Once the status of the stream is <code>ACTIVE</code>,
         * encryption begins for records written to the stream. </p> <p>API Limits: You can
         * successfully apply a new AWS KMS key for server-side encryption 25 times in a
         * rolling 24-hour period.</p> <p>Note: It can take up to five seconds after the
         * stream is in an <code>ACTIVE</code> status before all records written to the
         * stream are encrypted. After you enable encryption, you can verify that
         * encryption is applied by inspecting the API response from <code>PutRecord</code>
         * or <code>PutRecords</code>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/StartStreamEncryption">AWS
         * API Reference</a></p>
         */
        virtual Model::StartStreamEncryptionOutcome StartStreamEncryption(const Model::StartStreamEncryptionRequest& request) const;

        /**
         * <p>Enables or updates server-side encryption using an AWS KMS key for a
         * specified stream. </p> <p>Starting encryption is an asynchronous operation. Upon
         * receiving the request, Kinesis Streams returns immediately and sets the status
         * of the stream to <code>UPDATING</code>. After the update is complete, Kinesis
         * Streams sets the status of the stream back to <code>ACTIVE</code>. Updating or
         * applying encryption normally takes a few seconds to complete, but it can take
         * minutes. You can continue to read and write data to your stream while its status
         * is <code>UPDATING</code>. Once the status of the stream is <code>ACTIVE</code>,
         * encryption begins for records written to the stream. </p> <p>API Limits: You can
         * successfully apply a new AWS KMS key for server-side encryption 25 times in a
         * rolling 24-hour period.</p> <p>Note: It can take up to five seconds after the
         * stream is in an <code>ACTIVE</code> status before all records written to the
         * stream are encrypted. After you enable encryption, you can verify that
         * encryption is applied by inspecting the API response from <code>PutRecord</code>
         * or <code>PutRecords</code>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/StartStreamEncryption">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::StartStreamEncryptionOutcomeCallable StartStreamEncryptionCallable(const Model::StartStreamEncryptionRequest& request) const;

        /**
         * <p>Enables or updates server-side encryption using an AWS KMS key for a
         * specified stream. </p> <p>Starting encryption is an asynchronous operation. Upon
         * receiving the request, Kinesis Streams returns immediately and sets the status
         * of the stream to <code>UPDATING</code>. After the update is complete, Kinesis
         * Streams sets the status of the stream back to <code>ACTIVE</code>. Updating or
         * applying encryption normally takes a few seconds to complete, but it can take
         * minutes. You can continue to read and write data to your stream while its status
         * is <code>UPDATING</code>. Once the status of the stream is <code>ACTIVE</code>,
         * encryption begins for records written to the stream. </p> <p>API Limits: You can
         * successfully apply a new AWS KMS key for server-side encryption 25 times in a
         * rolling 24-hour period.</p> <p>Note: It can take up to five seconds after the
         * stream is in an <code>ACTIVE</code> status before all records written to the
         * stream are encrypted. After you enable encryption, you can verify that
         * encryption is applied by inspecting the API response from <code>PutRecord</code>
         * or <code>PutRecords</code>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/StartStreamEncryption">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void StartStreamEncryptionAsync(const Model::StartStreamEncryptionRequest& request, const StartStreamEncryptionResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Disables server-side encryption for a specified stream. </p> <p>Stopping
         * encryption is an asynchronous operation. Upon receiving the request, Kinesis
         * Streams returns immediately and sets the status of the stream to
         * <code>UPDATING</code>. After the update is complete, Kinesis Streams sets the
         * status of the stream back to <code>ACTIVE</code>. Stopping encryption normally
         * takes a few seconds to complete, but it can take minutes. You can continue to
         * read and write data to your stream while its status is <code>UPDATING</code>.
         * Once the status of the stream is <code>ACTIVE</code>, records written to the
         * stream are no longer encrypted by Kinesis Streams. </p> <p>API Limits: You can
         * successfully disable server-side encryption 25 times in a rolling 24-hour
         * period. </p> <p>Note: It can take up to five seconds after the stream is in an
         * <code>ACTIVE</code> status before all records written to the stream are no
         * longer subject to encryption. After you disabled encryption, you can verify that
         * encryption is not applied by inspecting the API response from
         * <code>PutRecord</code> or <code>PutRecords</code>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/StopStreamEncryption">AWS
         * API Reference</a></p>
         */
        virtual Model::StopStreamEncryptionOutcome StopStreamEncryption(const Model::StopStreamEncryptionRequest& request) const;

        /**
         * <p>Disables server-side encryption for a specified stream. </p> <p>Stopping
         * encryption is an asynchronous operation. Upon receiving the request, Kinesis
         * Streams returns immediately and sets the status of the stream to
         * <code>UPDATING</code>. After the update is complete, Kinesis Streams sets the
         * status of the stream back to <code>ACTIVE</code>. Stopping encryption normally
         * takes a few seconds to complete, but it can take minutes. You can continue to
         * read and write data to your stream while its status is <code>UPDATING</code>.
         * Once the status of the stream is <code>ACTIVE</code>, records written to the
         * stream are no longer encrypted by Kinesis Streams. </p> <p>API Limits: You can
         * successfully disable server-side encryption 25 times in a rolling 24-hour
         * period. </p> <p>Note: It can take up to five seconds after the stream is in an
         * <code>ACTIVE</code> status before all records written to the stream are no
         * longer subject to encryption. After you disabled encryption, you can verify that
         * encryption is not applied by inspecting the API response from
         * <code>PutRecord</code> or <code>PutRecords</code>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/StopStreamEncryption">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::StopStreamEncryptionOutcomeCallable StopStreamEncryptionCallable(const Model::StopStreamEncryptionRequest& request) const;

        /**
         * <p>Disables server-side encryption for a specified stream. </p> <p>Stopping
         * encryption is an asynchronous operation. Upon receiving the request, Kinesis
         * Streams returns immediately and sets the status of the stream to
         * <code>UPDATING</code>. After the update is complete, Kinesis Streams sets the
         * status of the stream back to <code>ACTIVE</code>. Stopping encryption normally
         * takes a few seconds to complete, but it can take minutes. You can continue to
         * read and write data to your stream while its status is <code>UPDATING</code>.
         * Once the status of the stream is <code>ACTIVE</code>, records written to the
         * stream are no longer encrypted by Kinesis Streams. </p> <p>API Limits: You can
         * successfully disable server-side encryption 25 times in a rolling 24-hour
         * period. </p> <p>Note: It can take up to five seconds after the stream is in an
         * <code>ACTIVE</code> status before all records written to the stream are no
         * longer subject to encryption. After you disabled encryption, you can verify that
         * encryption is not applied by inspecting the API response from
         * <code>PutRecord</code> or <code>PutRecords</code>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/StopStreamEncryption">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void StopStreamEncryptionAsync(const Model::StopStreamEncryptionRequest& request, const StopStreamEncryptionResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;

        /**
         * <p>Updates the shard count of the specified stream to the specified number of
         * shards.</p> <p>Updating the shard count is an asynchronous operation. Upon
         * receiving the request, Kinesis Streams returns immediately and sets the status
         * of the stream to <code>UPDATING</code>. After the update is complete, Kinesis
         * Streams sets the status of the stream back to <code>ACTIVE</code>. Depending on
         * the size of the stream, the scaling action could take a few minutes to complete.
         * You can continue to read and write data to your stream while its status is
         * <code>UPDATING</code>.</p> <p>To update the shard count, Kinesis Streams
         * performs splits or merges on individual shards. This can cause short-lived
         * shards to be created, in addition to the final shards. We recommend that you
         * double or halve the shard count, as this results in the fewest number of splits
         * or merges.</p> <p>This operation has the following limits, which are per region
         * per account unless otherwise noted. You cannot:</p> <ul> <li> <p>Scale more than
         * twice per rolling 24 hour period</p> </li> <li> <p>Scale up to double your
         * current shard count</p> </li> <li> <p>Scale down below half your current shard
         * count</p> </li> <li> <p>Scale up to more 500 shards in a stream</p> </li> <li>
         * <p>Scale a stream with more than 500 shards down unless the result is less than
         * 500 shards</p> </li> <li> <p>Scale up more the shard limits for your account</p>
         * </li> <li> <p/> </li> </ul> <p>For the default limits for an AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase a
         * limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/UpdateShardCount">AWS
         * API Reference</a></p>
         */
        virtual Model::UpdateShardCountOutcome UpdateShardCount(const Model::UpdateShardCountRequest& request) const;

        /**
         * <p>Updates the shard count of the specified stream to the specified number of
         * shards.</p> <p>Updating the shard count is an asynchronous operation. Upon
         * receiving the request, Kinesis Streams returns immediately and sets the status
         * of the stream to <code>UPDATING</code>. After the update is complete, Kinesis
         * Streams sets the status of the stream back to <code>ACTIVE</code>. Depending on
         * the size of the stream, the scaling action could take a few minutes to complete.
         * You can continue to read and write data to your stream while its status is
         * <code>UPDATING</code>.</p> <p>To update the shard count, Kinesis Streams
         * performs splits or merges on individual shards. This can cause short-lived
         * shards to be created, in addition to the final shards. We recommend that you
         * double or halve the shard count, as this results in the fewest number of splits
         * or merges.</p> <p>This operation has the following limits, which are per region
         * per account unless otherwise noted. You cannot:</p> <ul> <li> <p>Scale more than
         * twice per rolling 24 hour period</p> </li> <li> <p>Scale up to double your
         * current shard count</p> </li> <li> <p>Scale down below half your current shard
         * count</p> </li> <li> <p>Scale up to more 500 shards in a stream</p> </li> <li>
         * <p>Scale a stream with more than 500 shards down unless the result is less than
         * 500 shards</p> </li> <li> <p>Scale up more the shard limits for your account</p>
         * </li> <li> <p/> </li> </ul> <p>For the default limits for an AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase a
         * limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/UpdateShardCount">AWS
         * API Reference</a></p>
         *
         * returns a future to the operation so that it can be executed in parallel to other requests.
         */
        virtual Model::UpdateShardCountOutcomeCallable UpdateShardCountCallable(const Model::UpdateShardCountRequest& request) const;

        /**
         * <p>Updates the shard count of the specified stream to the specified number of
         * shards.</p> <p>Updating the shard count is an asynchronous operation. Upon
         * receiving the request, Kinesis Streams returns immediately and sets the status
         * of the stream to <code>UPDATING</code>. After the update is complete, Kinesis
         * Streams sets the status of the stream back to <code>ACTIVE</code>. Depending on
         * the size of the stream, the scaling action could take a few minutes to complete.
         * You can continue to read and write data to your stream while its status is
         * <code>UPDATING</code>.</p> <p>To update the shard count, Kinesis Streams
         * performs splits or merges on individual shards. This can cause short-lived
         * shards to be created, in addition to the final shards. We recommend that you
         * double or halve the shard count, as this results in the fewest number of splits
         * or merges.</p> <p>This operation has the following limits, which are per region
         * per account unless otherwise noted. You cannot:</p> <ul> <li> <p>Scale more than
         * twice per rolling 24 hour period</p> </li> <li> <p>Scale up to double your
         * current shard count</p> </li> <li> <p>Scale down below half your current shard
         * count</p> </li> <li> <p>Scale up to more 500 shards in a stream</p> </li> <li>
         * <p>Scale a stream with more than 500 shards down unless the result is less than
         * 500 shards</p> </li> <li> <p>Scale up more the shard limits for your account</p>
         * </li> <li> <p/> </li> </ul> <p>For the default limits for an AWS account, see <a
         * href="http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html">Streams
         * Limits</a> in the <i>Amazon Kinesis Streams Developer Guide</i>. To increase a
         * limit, <a
         * href="http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html">contact
         * AWS Support</a>.</p><p><h3>See Also:</h3>   <a
         * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/UpdateShardCount">AWS
         * API Reference</a></p>
         *
         * Queues the request into a thread executor and triggers associated callback when operation has finished.
         */
        virtual void UpdateShardCountAsync(const Model::UpdateShardCountRequest& request, const UpdateShardCountResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context = nullptr) const;


    private:
      void init(const Aws::Client::ClientConfiguration& clientConfiguration);

        /**Async helpers**/
        void AddTagsToStreamAsyncHelper(const Model::AddTagsToStreamRequest& request, const AddTagsToStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void CreateStreamAsyncHelper(const Model::CreateStreamRequest& request, const CreateStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void DecreaseStreamRetentionPeriodAsyncHelper(const Model::DecreaseStreamRetentionPeriodRequest& request, const DecreaseStreamRetentionPeriodResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void DeleteStreamAsyncHelper(const Model::DeleteStreamRequest& request, const DeleteStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void DescribeLimitsAsyncHelper(const Model::DescribeLimitsRequest& request, const DescribeLimitsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void DescribeStreamAsyncHelper(const Model::DescribeStreamRequest& request, const DescribeStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void DescribeStreamSummaryAsyncHelper(const Model::DescribeStreamSummaryRequest& request, const DescribeStreamSummaryResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void DisableEnhancedMonitoringAsyncHelper(const Model::DisableEnhancedMonitoringRequest& request, const DisableEnhancedMonitoringResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void EnableEnhancedMonitoringAsyncHelper(const Model::EnableEnhancedMonitoringRequest& request, const EnableEnhancedMonitoringResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void GetRecordsAsyncHelper(const Model::GetRecordsRequest& request, const GetRecordsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void GetShardIteratorAsyncHelper(const Model::GetShardIteratorRequest& request, const GetShardIteratorResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void IncreaseStreamRetentionPeriodAsyncHelper(const Model::IncreaseStreamRetentionPeriodRequest& request, const IncreaseStreamRetentionPeriodResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void ListStreamsAsyncHelper(const Model::ListStreamsRequest& request, const ListStreamsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void ListTagsForStreamAsyncHelper(const Model::ListTagsForStreamRequest& request, const ListTagsForStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void MergeShardsAsyncHelper(const Model::MergeShardsRequest& request, const MergeShardsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void PutRecordAsyncHelper(const Model::PutRecordRequest& request, const PutRecordResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void PutRecordsAsyncHelper(const Model::PutRecordsRequest& request, const PutRecordsResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void RemoveTagsFromStreamAsyncHelper(const Model::RemoveTagsFromStreamRequest& request, const RemoveTagsFromStreamResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void SplitShardAsyncHelper(const Model::SplitShardRequest& request, const SplitShardResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void StartStreamEncryptionAsyncHelper(const Model::StartStreamEncryptionRequest& request, const StartStreamEncryptionResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void StopStreamEncryptionAsyncHelper(const Model::StopStreamEncryptionRequest& request, const StopStreamEncryptionResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;
        void UpdateShardCountAsyncHelper(const Model::UpdateShardCountRequest& request, const UpdateShardCountResponseReceivedHandler& handler, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) const;

      Aws::String m_uri;
      std::shared_ptr<Aws::Utils::Threading::Executor> m_executor;
  };

} // namespace Kinesis
} // namespace Aws
