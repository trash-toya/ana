// @ts-check
import eslint from "@eslint/js";
import globals from "globals";

export default [
  eslint.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: 2024,
      sourceType: "module",
      globals: { ...globals.node },
    },
    rules: {
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": "off", // enabled per-workspace when TS rules load
      eqeqeq: ["error", "always"],
      "no-console": "off",
      "prefer-const": "error",
    },
  },
  {
    ignores: ["node_modules/", "**/dist/**", "coverage/", "docs/", "ops/"],
  },
];
