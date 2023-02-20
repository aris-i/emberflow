import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;

type BaseEntity = {
    name?: string;
    description?: string;
    dateCreated?: Timestamp;
    dateUpdated?: Timestamp;
}

type RootEntity = {
    users: User[];
    countries: Country[];
}

type User = BaseEntity & {
    organizations: Organization[];
};

type Organization = BaseEntity & {
    projects: Project[];
    members: Member[];
    forms: Form[];
    assets: Asset[];
};

type Project = BaseEntity & {
    accessList: ProjectAccessList[];
};

type ProjectAccessList = BaseEntity & {
    someField: string;
};

type Member = BaseEntity & {
    someField: string;
};

type Form = BaseEntity & {
    someField: string;
};

type Asset = BaseEntity & {
    accessList: AssetAccessList[];
};

type AssetAccessList = BaseEntity & {
    someField: string;
};

type Country = BaseEntity & {
    someField: string;
};

export type Entity = RootEntity | User | Organization | Project | ProjectAccessList | Member | Form | Asset | AssetAccessList | Country;
