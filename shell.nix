with (import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/release-24.05.tar.gz") {});

pkgs.mkShell {
	buildInputs = [
		rust-bin.nightly."2024-06-29".complete
		cargo-outdated

		openssl
		pkg-config
	];
}
