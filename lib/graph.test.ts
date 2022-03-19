import { Edge, makeGraph, topsort } from "./graph";
import assert from "assert";
import { testGraph } from "./test/graph";

describe("graph", () => {
	it("should order cyclic graphs", () => {
		const nodes = ["A", "B", "C", "D"];
		const edges = [
			{
				from: "A",
				to: "B",
				label: "AB",
			},
			{
				from: "B",
				to: "C",
				label: "BC",
			},
			{
				from: "C",
				to: "A",
				label: "AC",
			},
			{
				from: "B",
				to: "D",
				label: "BD",
			},
		];

		const graph = makeGraph(nodes, edges);
		const ordered = topsort(nodes[0], graph);
		assert.deepStrictEqual(ordered, ["A", "B", "D", "C"]);
	});

	it("should skip isolated vertices", function () {
		this.timeout(500);

		const nodes = ["A", "B"];
		const edges = Array<Edge<string>>();

		const graph = makeGraph(nodes, edges);
		const ordered = topsort(nodes[0], graph);
		assert.deepStrictEqual(ordered, ["A"]);
	});

	it("should calculate topographical sort", () => {
		const ordered = topsort(testGraph.nodes[0], testGraph);
		assert.deepStrictEqual(ordered, ["A", "C", "B", "D", "E"]);
	});
});
