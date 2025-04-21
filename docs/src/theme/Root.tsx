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
    createPolicy(name: string, rules: {
      createHTML?: (input: string) => string;
      createScript?: (input: string) => string;
      createScriptURL?: (input: string) => string;
    }): TrustedTypePolicy;
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
	const tt = (window as any).trustedTypes;

    if (
      tt &&
      typeof tt.createPolicy === 'function' &&
      typeof tt.getPolicy === 'function' &&
      !tt.getPolicy('ff#webpack')
    ) {
	  // Create a policy to allow the use of trusted types in the application
	  tt.createPolicy('ff#webpack', {
        createHTML: (input: any) => input,
        createScript: (input: any) => input,
        createScriptURL: (input: any) => input,
      });
    } else {
		console.warn(
			"Trusted Types are not supported in this browser. Please consider using a browser that supports Trusted Types for better security."
		);
	}
  }, []);

	return <>{children}</>;
}
