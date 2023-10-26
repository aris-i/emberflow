
export enum InternalEntity {
    Internal = "internal",
    ForDistribution = "for-distribution",
}
export const internalDbStructure = {
  "@internal": {
    [InternalEntity.Internal]: {
      distributions: {
        [InternalEntity.ForDistribution]: {},
      },
    },
  },
};

