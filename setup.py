import sys

if "travis_deploy" in sys.argv:
    print(sys.argv)
    import build_packages
    sys.exit(build_packages.travis_build_package())
else:
    raise ValueError("Setup file is written to support travis publish.")
    