// You should import from the path to the ember-flow package in your project
import {view} from "../utils/db-structure";

export enum Entity {
    User = "user", // do not delete
    // Add your custom entities below
    Feed = "feed",
    Friend = "friend",
    Game = "game",
    Algolia = "algolia",
    Topic = "topic",
    TopicAlgolia = "topic-algolia",
}

// Map your custom entities to dbStructure below.
// Do not remove users and [Entity.User]
// by default, view matches the id attribute of the view so make the sure that a view has an id
export const dbStructure = {
  push: {
    [Entity.Algolia]: {
      topics: {
        [Entity.TopicAlgolia]: {
          [view(Entity.Topic, ["name", "summary", "type"])]: {},
        },
      },
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
    },
  },
};
