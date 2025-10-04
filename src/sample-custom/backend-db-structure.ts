import {propView, view} from "../utils/db-structure";

export enum Entity {
  User = "user", // do not delete
  Topic = "topic",
  Thread = "thread",
  Message = "message",
  AskJarisMessage = "askJarisMessage",
  PerfTest = "perfTest",
  PerfData = "perfData",
  ToDoView = "todoView",
  PollOption = "pollOption",
  PollVote = "pollVote",
  UserInvite = "userInvite",
  TopicInvite = "topicInvite",
  Discover = "discover",
  DiscoverLike = "discoverLike",
  Activity = "activity",
  UserCollaborator = "userCollaborator",
  SubTopicView = "subtopicView",
  BackgroundProcess = "backgroundProcess",
  ParentTopic = "parentTopic",
  Ticket = "ticket",
  MenuItem = "menuItem",
  MenuItemStockHistory = "menuItemStockHistory",
  PreparationArea = "preparationArea",
  Ingredient = "ingredient",
  RecipeCategory = "recipeCategory",
  Order = "order",
  OrderMenuItem = "orderMenuItem",
  PreparationMenuItem = "preparationMenuItem",
  RestaurantStaff = "restaurantStaff",
  Table = "table",
  Customer = "customer",
  Discount = "discount",
  BatchProcess = "batchProcess",
  RecentIngredient = "recentIngredient",
  StatisticTimeframe = "statisticTimeframe",
  SalesStatistic = "salesStatistic",
  Expense = "expense",
  ExpenseCategory = "expenseCategory",
  ExpenseStatistic = "expenseStatistic",
  RecipeIngredient = "recipeIngredient",
  RecipeIngredientView = "recipeIngredientView",
  FinancialAccount = "financialAccount",
  FinancialTransaction = "financialTransaction",
  Transfer = "transfer",
  Bank = "bank",
  RestaurantDevice = "restaurantDevice",
  Broadcast = "broadcast",
  RestaurantSubscriptionPlan = "restaurantSubscriptionPlan",
  SubscriptionBilling = "subscriptionBilling",
  CashierLocation = "cashierLocation",
  Printer = "printer",
  EmailInvite = "emailInvite",
  PreparationAreaSound = "preparationAreaSound",
}

const userViewProps = [
  "username",
  "lastName",
  "firstName",
  "avatarUrl",
  "status",
];

// Note: props should only contain 30 items
const topicViewProps = [
  "title",
  "summary",
  "shortSummary",
  "private",
  "status",
  "dueDate",
  "maxDueDate",
  "createdBy",
  "createdById",
  "assignedTo",
  "assignedToId",
  "totalLeafTodos",
  "completedLeafTodos",
  "totalTodos",
  "completedTodos",
  "notStartedTodos",
  "totalSubtopics",
  "yesterdayCompletedTodos",
  "yesterdayCompletedTodoCountUpdatedAt",
  "reason",
  "reasonId",
  "reasonById",
  "@statusChangesThreadCreated",
  "parentTopicIds",
  "@collaborators",
  "collaborators",
  "parentTopicCreatorIds",
  "parentTopicAssigneeIds",
  "ogIds",
  "patchedCollaboratorsArrayMap",
];

const parentTopicViewProps = [
  "title",
  "assignedToId",
];

const recipeViewProps = [
  "type",
  "instructions",
  "ingredients",
  "basicCost",
  "preparationTime",
  "servingsCount",
  "tags",
];

const ingredientViewProps = [
  "title",
  "titleLowerCase",
  "unitMeasurement",
  "calories",
];

const recipeIngredientViewProps = [
  "amount",
  "ingredient",
];

const preparationAreaViewProps = [
  "title",
  "staffCount",
  "notStartedCount",
  "inProgressCount",
  "estimatedTimeToComplete",
  "status",
  "etcLastUpdatedAt",
];

const orderMenuItemViewProps = [
  "mode",
  "status",
  "priority",
  "customerInstruction",
  "orderCustomerInstruction",
  "notStartedCount",
  "quantity",
  "takeOutQuantity",
  "inProgressCount",
  "readyToServeCount",
  "servedCount",
  "inProgressAt",
  "readyToServeAt ",
  "timeInQueue",
  "preparationTime",
  "servingTime",
  "totalWaitingTime",
  "estimatedTimeToComplete",
  "progress",
  "assignedToId",
  "staff",
  "etcLastUpdatedAt",
  "initialEstimatedTimeToComplete",
  "idealPreparationTime",
];

const cashierLocationViewProps = [
  "name",
];

const inviteTopicViewProps = [
  "title",
  "@collaborators",
  "description",
  "imageUrl",
  "imageLastUpdatedAt",
  "logoUrl",
  "logoLastUpdatedAt",
];

const emailInviteTopicViewProps = [
  "title",
  "@followers",
  "description",
  "imageUrl",
  "imageLastUpdatedAt",
  "logoUrl",
  "@collaborators",
  "logoLastUpdatedAt",
];

// Map your custom entities to dbStructure below.
// Do not remove users and [Entity.User]
// by default, view matches the id attribute of the view so make the
// sure that a view has an id
export const dbStructure = {
  tickets: {
    [Entity.Ticket]: {},
  },
  backgroundProcesses: {
    [Entity.BackgroundProcess]: {},
  },
  users: {
    [Entity.User]: {
      collaborators: {
        [Entity.UserCollaborator]: [view(Entity.User, userViewProps)],
      },
      activities: {
        [Entity.Activity]: {},
      },
      invites: {
        [Entity.UserInvite]: {
          topic: [propView("map", Entity.Topic, inviteTopicViewProps)],
        },
      },
      askJaris: {
        [Entity.AskJarisMessage]: {},
      },
      perfTests: {
        [Entity.PerfTest]: {
          dataSet: {
            [Entity.PerfData]: {},
          },
        },
      },
      discover: {
        [Entity.Discover]: {
          likes: {
            [Entity.DiscoverLike]: {},
          },
        },
      },
    },
  },
  topics: {
    [Entity.Topic]: {
      // utility entity
      batchProcesses: {
        [Entity.BatchProcess]: {},
      },
      // global
      todos: {
        [Entity.ToDoView]: [view(Entity.Topic, topicViewProps)],
      },
      collaborators: [propView("array-map", Entity.User, userViewProps)],
      parentTopics: {
        [Entity.ParentTopic]: [view(Entity.Topic, parentTopicViewProps)],
      },
      threads: {
        [Entity.Thread]: {},
      },
      messages: {
        [Entity.Message]: {
          votes: {
            [Entity.PollVote]: {},
          },
          options: {
            [Entity.PollOption]: {},
          },
        },
      },
      invites: {
        [Entity.TopicInvite]: {
          topic: [propView("map", Entity.Topic, inviteTopicViewProps)],
        },
      },
      subtopics: {
        [Entity.SubTopicView]: [view(Entity.Topic, topicViewProps)],
      },
      // restaurant topic
      menuItems: {
        [Entity.MenuItem]: {
          ingredients: {
            [Entity.RecipeIngredientView]: [view(
              Entity.RecipeIngredient,
              recipeIngredientViewProps,
              {syncCreate: true}
            )],
          },
          preparationArea: [propView(
            "map", Entity.PreparationArea, ["title"], {}, "0.8.01"
          )],
          category: [propView(
            "map", Entity.RecipeCategory, ["title"], {}, "0.8.01"
          )],
          recipe: [propView("map", Entity.Topic, recipeViewProps)],
          history: {
            [Entity.MenuItemStockHistory]: {},
          },
        },
      },
      orders: {
        [Entity.Order]: {
          menus: {
            [Entity.OrderMenuItem]: {},
          },
        },
      },
      preparationAreas: {
        [Entity.PreparationArea]: {
          menus: {
            [Entity.PreparationMenuItem]: {
              orderMenuItem: [propView(
                "map", Entity.OrderMenuItem, orderMenuItemViewProps)],
              ingredients: [propView(
                "array-map", Entity.RecipeIngredient,
                recipeIngredientViewProps)],
              recipe: [propView("map", Entity.Topic, recipeViewProps)],
            },
          },
        },
      },
      categories: {
        [Entity.RecipeCategory]: {},
      },
      staff: {
        [Entity.RestaurantStaff]: {},
      },
      customers: {
        [Entity.Customer]: {},
      },
      discounts: {
        [Entity.Discount]: {},
      },
      recentIngredients: {
        [Entity.RecentIngredient]: [view(Entity.Topic, ingredientViewProps )],
      },
      expenses: {
        [Entity.Expense]: {},
      },
      expenseCategories: {
        [Entity.ExpenseCategory]: {},
      },
      statistics: {
        [Entity.StatisticTimeframe]: {
          sales: {
            [Entity.SalesStatistic]: {},
          },
          expenses: {
            [Entity.ExpenseStatistic]: {},
          },
        },
      },
      accounts: {
        [Entity.FinancialAccount]: {
          transactions: {
            [Entity.FinancialTransaction]: {},
          },
        },
      },
      transfers: {
        [Entity.Transfer]: {},
      },
      banks: {
        [Entity.Bank]: {},
      },
      devices: {
        [Entity.RestaurantDevice]: {
          cashierLocation: [
            propView("map", Entity.CashierLocation, cashierLocationViewProps)],
        },
      },
      broadcasts: {
        [Entity.Broadcast]: {},
      },
      preparationAreaViews: [propView(
        "array-map", Entity.PreparationArea, preparationAreaViewProps)],
      subscriptionBillings: {
        [Entity.SubscriptionBilling]: {},
      },
      cashierLocations: {
        [Entity.CashierLocation]: {
          tables: {
            [Entity.Table]: {},
          },
        },
      },
      preparationAreaSounds: {
        [Entity.PreparationAreaSound]: {},
      },
      // recipe topic
      ingredients: {
        [Entity.RecipeIngredient]: {
          ingredient: [propView("map", Entity.Topic, ingredientViewProps)],
        },
      },
      printers: {
        [Entity.Printer]: {
          cashierLocation: [propView("map", Entity.CashierLocation, ["name"])],
        },
      },
      // ingredients: view(Entity.Topic, recipeIngredientViewProps),
    },
  },
  restaurantSubscriptionPlans: {
    [Entity.RestaurantSubscriptionPlan]: {},
  },
  invites: {
    [Entity.EmailInvite]: {
      invitedBy: [propView("array-map", Entity.User, userViewProps)],
      topic: [propView("array-map", Entity.Topic, emailInviteTopicViewProps)],
    },
  },
};
