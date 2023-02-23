export enum Entity {
    User = "user", // do not delete
    // Add your custom entities below
    YourCustomEntity = "yourCustomEntity",
}

// Map your custom entities to dbStructure below.  Do not remove users and [Entity.User]
export const dbStructure = {
  users: {
    [Entity.User]: {
      customs: {
        [Entity.YourCustomEntity]: {},
      },
    },
  },
};
