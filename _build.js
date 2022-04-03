#!/usr/bin/env node

const fs = require("fs-extra");
const path = require("path");

const main = async () => {
	const command = process.argv[2] ?? "build";

	if (command === "clean") {
		fs.emptyDirSync("dist");
		return;
	}

	if (command === "build") {
		fs.ensureDirSync("dist")
		fs.copySync("README.md", path.join("dist", "README.md"))
		fs.copySync("package.json", path.join("dist", "package.json"))
		return;
	}
};

main();
