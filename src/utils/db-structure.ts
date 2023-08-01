function view(entity: string, props: string[], options?: Record<string, any>): string {
  // concat entity and props
  return `View:${entity}:${props.join(",")}`;
}

export {view};
