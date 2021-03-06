﻿using System;
using System.Threading.Tasks;
using Chill;
using FluentAssertions;
using Xunit;

namespace LiquidProjections.Specs
{
    namespace EventMapSpecs
    {
        public class When_an_event_is_mapped_as_an_update : GivenSubject<EventMap<ProductCatalogEntry, ProjectionContext>>
        {
            private ProductCatalogEntry projection;

            public When_an_event_is_mapped_as_an_update()
            {
                Given(() =>
                {
                    Subject.ForwardUpdatesTo(async (key, context, projector) =>
                    {
                        projection = new ProductCatalogEntry
                        {
                            Id = key,
                        };

                        await projector(projection, context);
                    });

                    Subject.Map<ProductAddedToCatalogEvent>().AsUpdateOf(e => e.ProductKey).Using((p, e, ctx) =>
                    {
                        p.Category = e.Category;

                        return Task.FromResult(0);
                    });
                });

                When(async () =>
                {
                    Func<ProjectionContext, Task> handler = Subject.GetHandler(new ProductAddedToCatalogEvent
                    {
                        Category = "Hybrids",
                        ProductKey = "c350E"
                    });

                    await handler(new ProjectionContext());
                });
            }

            [Fact]
            public void It_should_properly_pass_the_mapping_to_the_updating_handler()
            {
                projection.ShouldBeEquivalentTo(new
                {
                    Id = "c350E",
                    Category = "Hybrids",
                    Deleted = false
                });
            }
        }

        public class When_an_event_is_mapped_as_a_delete : GivenSubject<EventMap<ProductCatalogEntry, ProjectionContext>>
        {
            private ProductCatalogEntry projection;

            public When_an_event_is_mapped_as_a_delete()
            {
                Given(() =>
                {
                    Subject.ForwardDeletesTo((key, context) =>
                    {
                        projection = new ProductCatalogEntry
                        {
                            Id = key,
                            Deleted = true
                        };

                        return Task.FromResult(0);
                    });

                    Subject.Map<ProductDiscontinuedEvent>().AsDeleteOf(e => e.ProductKey);
                });

                When(async () =>
                {
                    Func<ProjectionContext, Task> handler = Subject.GetHandler(new ProductDiscontinuedEvent
                    {
                        ProductKey = "c350E"
                    });

                    await handler(new ProjectionContext());
                });
            }

            [Fact]
            public void It_should_properly_pass_the_mapping_to_the_deleting_handler()
            {
                projection.ShouldBeEquivalentTo(new
                {
                    Id = "c350E",
                    Category = (string) null,
                    Deleted = true
                });
            }
        }

        public class When_an_event_is_mapped_as_a_custom_action : GivenSubject<EventMap<ProductCatalogEntry, ProjectionContext>>
        {
            private string involvedKey;

            public When_an_event_is_mapped_as_a_custom_action()
            {
                Given(() =>
                {
                    Subject.ForwardCustomActionsTo((context, projector) => projector(context));

                    Subject.Map<ProductDiscontinuedEvent>().As((@event, context) =>
                    {
                        involvedKey = @event.ProductKey;

                        return Task.FromResult(0);
                    });
                });

                When(async () =>
                {
                    Func<ProjectionContext, Task> handler = Subject.GetHandler(new ProductDiscontinuedEvent
                    {
                        ProductKey = "c350E"
                    });

                    await handler(new ProjectionContext());
                });
            }

            [Fact]
            public void It_should_properly_pass_the_mapping_to_the_custom_handler()
            {
                involvedKey.Should().Be("c350E");
            }
        }

        public class When_a_condition_is_not_met : GivenSubject<EventMap<ProductCatalogEntry, ProjectionContext>>
        {
            private ProductCatalogEntry projection;

            public When_a_condition_is_not_met()
            {
                Given(() =>
                {
                    Subject.ForwardUpdatesTo(async (key, context, projector) =>
                    {
                        projection = new ProductCatalogEntry
                        {
                            Id = key,
                        };

                        await projector(projection, context);
                    });

                    Subject.Map<ProductAddedToCatalogEvent>().When(e => e.Category == "Electric").AsUpdateOf(e => e.ProductKey).Using((p, e, ctx) =>
                    {
                        p.Category = e.Category;

                        return Task.FromResult(0);
                    });
                });

                When(async () =>
                {
                    Func<ProjectionContext, Task> handler = Subject.GetHandler(new ProductAddedToCatalogEvent
                    {
                        Category = "Hybrids",
                        ProductKey = "c350E"
                    });

                    await handler(new ProjectionContext());
                });
            }

            [Fact]
            public void It_should_not_invoke_any_handler()
            {
                projection.Should().BeNull();
            }
        }

        public class When_a_condition_is_met : GivenSubject<EventMap<ProductCatalogEntry, ProjectionContext>>
        {
            private ProductCatalogEntry projection;

            public When_a_condition_is_met()
            {
                Given(() =>
                {
                    Subject.ForwardUpdatesTo(async (key, context, projector) =>
                    {
                        projection = new ProductCatalogEntry
                        {
                            Id = key,
                        };

                        await projector(projection, context);
                    });

                    Subject.Map<ProductAddedToCatalogEvent>().When(e => e.Category == "Hybrids").AsUpdateOf(e => e.ProductKey).Using((p, e, ctx) =>
                    {
                        p.Category = e.Category;

                        return Task.FromResult(0);
                    });
                });

                When(async () =>
                {
                    Func<ProjectionContext, Task> handler = Subject.GetHandler(new ProductAddedToCatalogEvent
                    {
                        Category = "Hybrids",
                        ProductKey = "c350E"
                    });

                    await handler(new ProjectionContext());
                });
            }

            [Fact]
            public void It_should_invoke_the_right_handler()
            {
                projection.ShouldBeEquivalentTo(new
                {
                    Id = "c350E",
                    Category = "Hybrids",
                    Deleted = false
                });
            }
        }
        public class When_multiple_conditions_are_registered : GivenSubject<EventMap<ProductCatalogEntry, ProjectionContext>>
        {
            private Action action;

            public When_multiple_conditions_are_registered()
            {
                When(() =>
                {
                    action = () =>
                    {
                        Subject.Map<ProductAddedToCatalogEvent>()
                            .When(e => e.Category == "Hybrids")
                            .AsUpdateOf(e => e.ProductKey).Using((p, e, ctx) => p.Category = e.Category);

                        Subject.Map<ProductAddedToCatalogEvent>()
                            .When(e => e.Category == "Electrics")
                            .AsDeleteOf(e => e.ProductKey);
                    };
                });
            }

            [Fact]
            public void It_should_allow_all_of_them()
            {
                action.ShouldNotThrow();
            }
        }

        public class ProductCatalogEntry
        {
            public string Id { get; set; }
            public string Category { get; set; }
            public bool Deleted { get; set; }
        }

        public class ProductAddedToCatalogEvent
        {
            public string ProductKey { get; set; }
            public string Category { get; set; }

            public long Version { get; set; }
        }

        public class ProductDiscontinuedEvent
        {
            public string ProductKey { get; set; }
        }

        public class CategoryDiscontinuedEvent
        {
            public string Category { get; set; }
        }
    }
}