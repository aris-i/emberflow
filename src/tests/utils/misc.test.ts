import {deepEqual} from "../../utils/misc";
import {firestore} from "firebase-admin";
import Timestamp = firestore.Timestamp;
import GeoPoint = firestore.GeoPoint;

describe("deepEqual", () => {
  it("should correctly compare Firestore Field types", () => {
    const ts1 = Timestamp.fromDate(new Date(2023, 3, 1));
    const ts2 = Timestamp.fromDate(new Date(2023, 3, 1));
    const ts3 = Timestamp.fromDate(new Date(2023, 3, 2));

    const geo1 = new GeoPoint(37.422, -122.084);
    const geo2 = new GeoPoint(37.422, -122.084);
    const geo3 = new GeoPoint(37.426, -122.081);

    const array1 = [ts1, geo1];
    const array2 = [ts2, geo2];
    const array3 = [ts1, geo3];

    const obj1 = {timestamp: ts1, geoPoint: geo1};
    const obj2 = {timestamp: ts2, geoPoint: geo2};
    const obj3 = {timestamp: ts1, geoPoint: geo3};

    expect(deepEqual(ts1, ts2)).toBe(true);
    expect(deepEqual(ts1, ts3)).toBe(false);

    expect(deepEqual(geo1, geo2)).toBe(true);
    expect(deepEqual(geo1, geo3)).toBe(false);

    expect(deepEqual(array1, array2)).toBe(true);
    expect(deepEqual(array1, array3)).toBe(false);

    expect(deepEqual(obj1, obj2)).toBe(true);
    expect(deepEqual(obj1, obj3)).toBe(false);
  });
});
