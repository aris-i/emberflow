// You should import from the path to the ember-flow package in your project
import {view} from "../utils/db-structure";

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
}

// Map your custom entities to dbStructure below.
// Do not remove users and [Entity.User]
// by default, view matches the id attribute of the view so make the sure that a view has an id
export const dbStructure = {
  servers: {
    [Entity.Server]: {
      createdBy: view(Entity.User, ["name", "email"]),
      channels: {
        [Entity.Channel]: {
          "createdBy": view(Entity.User, ["name", "email"]),
          "members": {
            [view(Entity.User, ["name"])]: {},
          },
        },
      },
      members: {
        [Entity.Member]: {
          [view(Entity.User, ["name"])]: {},
        },
      },
      followers: [view(Entity.User, ["name", "email"])],
    },
  },
  users: {
    [Entity.User]: {
      feeds: {
        [Entity.Feed]: {
          createdBy: view(Entity.User, ["name", "email"]),
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
    },
  },
};
