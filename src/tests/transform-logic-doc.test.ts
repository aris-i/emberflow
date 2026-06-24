import { transformLogicResultDocIfNeeded } from '../index';
import * as index from '../index';

describe('transformLogicResultDocIfNeeded', () => {
    let originalEntityViewDefinitions: any;
    let originalDocPathsRegex: any;

    beforeAll(() => {
        originalEntityViewDefinitions = { ...index.entityViewDefinitions };
        originalDocPathsRegex = { ...index.docPathsRegex };
    });

    afterAll(() => {
        // @ts-ignore
        Object.keys(index.entityViewDefinitions).forEach(key => delete index.entityViewDefinitions[key]);
        // @ts-ignore
        Object.assign(index.entityViewDefinitions, originalEntityViewDefinitions);

        // @ts-ignore
        Object.keys(index.docPathsRegex).forEach(key => delete index.docPathsRegex[key]);
        // @ts-ignore
        Object.assign(index.docPathsRegex, originalDocPathsRegex);
    });

    it('should transform LogicResultDoc when destProp is present and no viewDefinition exists', () => {
        // Setup entityViewDefinitions
        // @ts-ignore
        index.entityViewDefinitions['myEntity'] = {};
        // @ts-ignore
        index.docPathsRegex['myEntity'] = /^myCollection\/([^/]+)$/;

        const doc = {
            dstPath: 'myCollection/myDoc#myProp',
            doc: { prop1: 'hello' },
            action: 'create',
            priority: 'normal'
        };

        // @ts-ignore
        const result = transformLogicResultDocIfNeeded(doc as any);

        expect(result.dstPath).toBe('myCollection/myDoc');
        expect(result.doc).toEqual({ myProp: { prop1: 'hello' } });
    });

    it('should NOT transform LogicResultDoc when destProp is present and viewDefinition EXISTS', () => {
        // @ts-ignore
        index.entityViewDefinitions['myEntity'] = {
            'myProp': { destEntity: 'myEntity', destProp: { name: 'myProp', type: 'map' } } as any
        };
        // @ts-ignore
        index.docPathsRegex['myEntity'] = /^myCollection\/([^/]+)$/;

        const doc = {
            dstPath: 'myCollection/myDoc#myProp',
            doc: { prop1: 'hello' },
            action: 'create',
            priority: 'normal'
        };

        // @ts-ignore
        const result = transformLogicResultDocIfNeeded(doc as any);

        expect(result.dstPath).toBe('myCollection/myDoc#myProp');
        expect(result.doc).toEqual({ prop1: 'hello' });
    });

    it('should NOT transform LogicResultDoc when NO destProp is present', () => {
        const doc = {
            dstPath: 'myCollection/myDoc',
            doc: { prop1: 'hello' },
            action: 'create',
            priority: 'normal'
        };

        // @ts-ignore
        const result = transformLogicResultDocIfNeeded(doc as any);

        expect(result.dstPath).toBe('myCollection/myDoc');
        expect(result.doc).toEqual({ prop1: 'hello' });
    });
});
