// You should import from the path to the ember-flow package in your project
import {propView, view} from "../utils/db-structure";

export enum Entity {
    User = "user", // do not delete
    // Add your custom entities below
    Feed = "feed",
    Friend = "friend",
    Game = "game",
    Server = "server",
    Channel = "channel",
    Member = "member",
    Post = "post",
    Comment = "comment",
    Topic = "topic",
    Order = "order",
    MenuItem = "menuItem",
    OrderMenuItem = "orderMenuItem",
    PrepArea = "prepArea",
    PrepAreaMenuItem = "prepAreaMenuItem",
    RecipeIngredient = "recipeIngredient",
    MenuItemIngredient = "menuItemIngredient",
    Todos = "todos",
}

// Map your custom ent  ities to dbStructure below.
// Do not remove users and [Entity.User]
// by default, view matches the id attribute of the view so make the sure that a view has an id

export const dbStructure = {
  servers: {
    [Entity.Server]: {
      createdBy: propView("map", Entity.User, ["name", "email"]),
      channels: {
        [Entity.Channel]: {
          "createdBy": propView("map", Entity.User, ["name", "email"]),
          "members": {
            [propView("array-map", Entity.User, ["name"])]: {},
          },
        },
      },
      members: {
        [Entity.Member]: [view(Entity.User, ["name"])],
      },
      followers: [propView("array-map", Entity.User, ["name", "email"])],
    },
  },
  topics: {
    [Entity.Topic]: {
      ingredients: {
        [Entity.RecipeIngredient]: {},
      },
      menuItems: {
        [Entity.MenuItem]: {
          recipe: [
            propView("map", Entity.Topic, [], {}, "0.0.1"),
            propView("array-map", Entity.Topic, [], {}, "0.0.2"),
            view(Entity.Topic, [], {}, "0.0.3"),
            view(Entity.Server, [], {}, "0.0.4"),
          ],
          ingredients: {
            [Entity.MenuItemIngredient]: [
              view(Entity.RecipeIngredient, [], {syncCreate: true}, "0.0.1"),
              propView("map", Entity.RecipeIngredient, [], {syncCreate: true}, "0.0.2"),
              propView("array-map", Entity.RecipeIngredient, [], {syncCreate: true}, "0.0.3"),
              view(Entity.Topic, [], {syncCreate: true}, "0.0.4"),
            ],
          },
        },
      },
      createdBy: [propView("map", Entity.User, ["name", "email"])],
      orders: {
        [Entity.Order]: {
          createdBy: [propView("map", Entity.User, ["name", "email"])],
          menus: {
            [Entity.OrderMenuItem]: {
              createdBy: [propView("map", Entity.User, ["name", "email"])],
            },
          },
        },
      },
      prepAreas: {
        [Entity.PrepArea]: {
          "createdBy": [propView("map", Entity.User, ["name", "email"])],
          "menus": {
            [Entity.PrepAreaMenuItem]: {
              createdBy: [propView("map", Entity.User, ["name", "email"])],
            },
          },
        },
      },
    },
  },
  users: {
    [Entity.User]: {
      feeds: {
        [Entity.Feed]: {
          createdBy: [propView("map", Entity.User, ["name", "email"])],
        },
      },
      friends: {
        [Entity.Friend]: {
          [view(Entity.User, ["name", "email"])]: {},
          games: {
            game: {},
          },
        },
      },
      posts: {
        [Entity.Post]: {
          comments: {
            [Entity.Comment]: {},
          },
        },
      },
      todosCollection: {
        [Entity.Todos]: [
          view(Entity.Topic, ["title"], {}, "1.0.0"),
          view(Entity.Topic, ["title", "name"], {}, "2.0.0"),
        ],
      },
      todosArray: [propView("array-map", Entity.Topic, ["title"], {}, "2.0.0")],
      mainTopic: [propView("map", Entity.Topic, ["title"], {}, "3.0.0")],
    },
  },
};
