using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Threading.Tasks;
using Chill;
using FakeItEasy;
using FluentAssertions;

using LiquidProjections.NEventStore.Logging;

using NEventStore;
using NEventStore.Persistence;
using Xunit;
using Xunit.Abstractions;

namespace LiquidProjections.NEventStore.Specs
{
    namespace EventStoreClientSpecs
    {
        // TODO: Lots of subscriptions for the same checkpoint result in a single query when a new event is pushed
        // TODO: Multiple serialized subscriptions for the same checkpoint result in a single query when executed within the cache window
        // TODO: Multiple serialized subscriptions for the same checkpoint result in a multiple queries when executed outside the cache window
        // TODO: When disposing the adapter, no transactions should be published to subscribers anymore
        // TODO: When disposing the adapter, no queries must happend anymore
        // TODO: When disposing a subscription, no transactions should be published to the subscriber anymore

        public class When_the_persistency_engine_is_temporarily_unavailable : GivenSubject<NEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 1.Seconds();
            private Transaction actualTransaction;

            public When_the_persistency_engine_is_temporarily_unavailable()
            {
                Given(() =>
                {
                    UseThe((ICommit) new CommitBuilder().WithCheckpoint("123").Build());

                    var eventStore = A.Fake<IPersistStreams>();
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).Returns(new[] {The<ICommit>()});
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).Throws(new ApplicationException()).Once();

                    WithSubject(_ => new NEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => DateTime.UtcNow));
                });

                When(() =>
                {
                    Subject.Subscribe(null, transactions =>
                    {
                        actualTransaction = transactions.First();

                        return Task.FromResult(0);
                    });
                });
            }

            [Fact]
            public async Task Then_it_should_recover_automatically_after_its_polling_interval_expires()
            {
                do
                {
                    await Task.Delay(pollingInterval);
                }
                while (actualTransaction == null);

                actualTransaction.Id.Should().Be(The<ICommit>().CommitId.ToString());
            }
        }

        public class When_a_commit_is_persisted : GivenSubject<NEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 1.Seconds();
            private TaskCompletionSource<Transaction> transactionHandledSource = new TaskCompletionSource<Transaction>();
            
            public When_a_commit_is_persisted()
            {
                Given(() =>
                {
                    UseThe((ICommit) new CommitBuilder().WithCheckpoint("123").Build());

                    var eventStore = A.Fake<IPersistStreams>();
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).Returns(new[] {The<ICommit>()});

                    WithSubject(_ => new NEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => DateTime.UtcNow));
                });

                When(() =>
                {
                    Subject.Subscribe(null, transactions =>
                    {
                        transactionHandledSource.SetResult(transactions.First());

                        return Task.FromResult(0);
                    });
                });
            }

            [Fact]
            public async Task Then_it_should_convert_the_commit_details_to_a_transaction()
            {
                Transaction actualTransaction = await transactionHandledSource.Task;

                var commit = The<ICommit>();
                actualTransaction.Id.Should().Be(commit.CommitId.ToString());
                actualTransaction.Checkpoint.Should().Be(long.Parse(commit.CheckpointToken));
                actualTransaction.TimeStampUtc.Should().Be(commit.CommitStamp);
                actualTransaction.StreamId.Should().Be(commit.StreamId);

                actualTransaction.Events.ShouldBeEquivalentTo(commit.Events, options => options.ExcludingMissingMembers());
            }
        }

        public class When_there_are_no_more_commits : GivenSubject<NEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 500.Milliseconds();
            private DateTime utcNow = DateTime.UtcNow;
            private IPersistStreams eventStore;
            private TaskCompletionSource<object> eventStoreQueriedSource = new TaskCompletionSource<object>();

            public When_there_are_no_more_commits()
            {
                Given(() =>
                {
                    eventStore = A.Fake<IPersistStreams>();
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored))
                        .Invokes(_ =>
                        {
                            eventStoreQueriedSource.SetResult(null);
                        })
                        .Returns(new ICommit[0]);

                    WithSubject(_ => new NEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => utcNow));

                    Subject.Subscribe(1000, transactions => Task.FromResult(0));
                });

                When(async () =>
                {
                    await eventStoreQueriedSource.Task;
                });
            }

            [Fact]
            public void Then_it_should_wait_for_the_polling_interval_to_retry()
            {
                A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).MustHaveHappened(Repeated.Exactly.Once);

                utcNow = utcNow.Add(1.Seconds());

                A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).MustHaveHappened(Repeated.Exactly.Once);
            }
        }

        public class When_a_commit_is_already_projected : GivenSubject<NEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 1.Seconds();
            private TaskCompletionSource<Transaction> transactionHandledSource = new TaskCompletionSource<Transaction>();

            public When_a_commit_is_already_projected()
            {
                Given(() =>
                {
                    ICommit projectedCommit = new CommitBuilder().WithCheckpoint("123").Build();
                    ICommit unprojectedCommit = new CommitBuilder().WithCheckpoint("124").Build();

                    var eventStore = A.Fake<IPersistStreams>();
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).Returns(new[] {projectedCommit, unprojectedCommit});
                    A.CallTo(() => eventStore.GetFrom("123")).Returns(new[] {unprojectedCommit});

                    WithSubject(_ => new NEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => DateTime.UtcNow));
                });

                When(() =>
                {
                    Subject.Subscribe(123, transactions =>
                    {
                        transactionHandledSource.SetResult(transactions.First());

                        return Task.FromResult(0);
                    });
                });
            }

            [Fact]
            public async Task Then_it_should_convert_the_unprojected_commit_details_to_a_transaction()
            {
                Transaction actualTransaction = await transactionHandledSource.Task;

                actualTransaction.Checkpoint.Should().Be(124);
            }
        }

        public class When_disposing : GivenSubject<NEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 500.Milliseconds();
            private DateTime utcNow = DateTime.UtcNow;
            private IPersistStreams eventStore;

            public When_disposing()
            {
                Given(() =>
                {
                    eventStore = A.Fake<IPersistStreams>();
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).Returns(new ICommit[0]);

                    WithSubject(_ => new NEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => utcNow));

                    Subject.Subscribe(1000, transactions => Task.FromResult(0));
                });

                When(() => Subject.Dispose(), deferedExecution: true);
            }

            [Fact]
            public void Then_it_should_stop()
            {
                if (!Task.Run(() => WhenAction.ShouldNotThrow()).Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new InvalidOperationException("The adapter has not stopped in 10 seconds.");
                }
            }
        }

        public class When_disposing_subscription : GivenSubject<NEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 500.Milliseconds();
            private DateTime utcNow = DateTime.UtcNow;
            private IPersistStreams eventStore;
            private IDisposable subscription;

            public When_disposing_subscription()
            {
                Given(() =>
                {
                    eventStore = A.Fake<IPersistStreams>();
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored)).Returns(new ICommit[0]);

                    WithSubject(_ => new NEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => utcNow));

                    subscription = Subject.Subscribe(1000, transactions => Task.FromResult(0));
                });

                When(() => subscription.Dispose(), deferedExecution: true);
            }

            [Fact]
            public void Then_it_should_stop()
            {
                if (!Task.Run(() => WhenAction.ShouldNotThrow()).Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new InvalidOperationException("The subscription has not stopped in 10 seconds.");
                }
            }
        }

        public class When_multiple_subscribers_exist : GivenSubject<NEventStoreAdapter>
        {
            private readonly TimeSpan pollingInterval = 1.Seconds();
            private TaskCompletionSource<Exception>[] completionSources;

            public When_multiple_subscribers_exist(ITestOutputHelper output)
            {
                Given(() =>
                {
                    LogProvider.SetCurrentLogProvider(new TestLogProvider(output));

                    var eventStore = A.Fake<IPersistStreams>();
                    A.CallTo(() => eventStore.GetFrom(A<string>.Ignored))
                        .ReturnsLazily(call =>
                        {
                            string checkpointString = call.GetArgument<string>(0);
                            int checkPoint =  !string.IsNullOrEmpty(checkpointString) ? int.Parse(checkpointString) : 0;

                            if (checkPoint < 10000)
                            {
                                return Enumerable
                                    .Range(checkPoint + 1, 100)
                                    .Select(c => new CommitBuilder().WithCheckpoint(c.ToString()).Build())
                                    .ToArray();
                            }
                            else
                            {
                                return new ICommit[0];
                            }
                        });

                    WithSubject(_ => new NEventStoreAdapter(eventStore, 11, pollingInterval, 100, () => DateTime.UtcNow));
                });

                When(() =>
                {
                    completionSources = new TaskCompletionSource<Exception>[20];

                    for (int index = 0; index < completionSources.Length; index++)
                    {
                        var completionSource = new TaskCompletionSource<Exception>();

                        int temp = index;
                        Subject.Subscribe(null, async transactions =>
                        {
                            string id = temp.ToString();

                            if (transactions.Count > 1 && transactions.First().Checkpoint == transactions.Last().Checkpoint)
                            {
                                completionSource.SetResult(new InvalidOperationException(
                                    $"Subscriber {id} received an invalid page of transactions"));
                            }

                            if (transactions.Any(t => t.Checkpoint > 10000))
                            {
                                output.WriteLine($"Subscriber {id} received all transactions ");
                                completionSource.SetResult(null);
                            }

                            await Task.Delay(new Random().Next(0, 50));
                        }, index.ToString());

                        completionSources[index] = completionSource;
                    }
                });
            }

            [Fact]
            public async Task Then_no_exceptions_must_have_happened()
            {
                var exceptions = await Task.WhenAll(completionSources.Select(s => s.Task));
                foreach (Exception exception in exceptions)
                {
                    if (exception != null)
                    {
                        throw exception;
                    }
                }
            }

            private class TestLogProvider : ILogProvider
            {
                private readonly ITestOutputHelper testOutputHelper;

                public TestLogProvider(ITestOutputHelper testOutputHelper)
                {
                    this.testOutputHelper = testOutputHelper;
                }

                public Logger GetLogger(string name)
                {
                    return (logLevel, messageFunc, exception, formatParameters) =>
                    {
                        string message = messageFunc();

                        if ((formatParameters != null) && (formatParameters.Length > 1))
                        {
                            message = string.Format(message, formatParameters);
                        }

                        testOutputHelper.WriteLine(message);
                        return true;
                    };
                }

                public IDisposable OpenNestedContext(string message)
                {
                    return new NullDisposable();
                }

                public IDisposable OpenMappedContext(string key, string value)
                {
                    return new NullDisposable();
                }

                private class NullDisposable : IDisposable
                {
                    public void Dispose()
                    {
                    }
                }
            }
        }
    }
}