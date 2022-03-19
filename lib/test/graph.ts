import { makeGraph } from "../graph";

const nodes = ["A", "B", "C", "D", "E"];

const edges = [
	{
		from: "A",
		to: "B",
		label: "AB",
	},
	{
		from: "A",
		to: "C",
		label: "AC",
	},
	{
		from: "C",
		to: "A",
		label: "CA",
	},
	{
		from: "D",
		to: "A",
		label: "DA",
	},
	{
		from: "D",
		to: "E",
		label: "DE",
	},
	{
		from: "B",
		to: "E",
		label: "BE",
	},
	{
		from: "C",
		to: "D",
		label: "CD",
	},
	{
		from: "B",
		to: "D",
		label: "BD",
	},
];

export const testGraph = makeGraph(nodes, edges);
