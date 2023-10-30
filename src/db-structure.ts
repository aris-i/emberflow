
export enum InternalEntity {
    Internal = "internal",
    ForDistribution = "forDistribution",
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

