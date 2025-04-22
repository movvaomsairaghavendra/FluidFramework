/*!
 * Copyright (c) Microsoft Corporation and contributors. All rights reserved.
 * Licensed under the MIT License.
 */

import React, { useEffect } from "react";

export type RootProps = React.PropsWithChildren;

declare global {
	interface TrustedTypePolicy {
		createHTML(input: string): string;
		createScript(input: string): string;
		createScriptURL(input: string): string;
	}

	interface TrustedTypePolicyFactory {
		createPolicy(
			name: string,
			rules: {
				createHTML?: (input: string) => string;
				createScript?: (input: string) => string;
				createScriptURL?: (input: string) => string;
			},
		): TrustedTypePolicy;
		getPolicy(name: string): TrustedTypePolicy | undefined;
	}

	interface Window {
		trustedTypes?: TrustedTypePolicyFactory;
	}
}

/**
 * Root component of Docusaurus's React tree.
 * Guaranteed to never unmount.
 *
 * @see {@link https://docusaurus.io/docs/swizzling#wrapper-your-site-with-root}
 */
export default function Root({ children }: RootProps): React.ReactElement {
	useEffect(() => {
		try {
			window.trustedTypes?.createPolicy?.("ff#webpack", {
				createHTML: (s) => s,
				createScript: (s) => s,
				createScriptURL: (s) => s,
			});
		} catch (error) {
			console.warn(error, "Trusted Types is not supported in this browser");
		}
	}, []);

	return <>{children}</>;
}
