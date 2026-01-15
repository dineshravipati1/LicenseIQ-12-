import { useQuery } from "@tanstack/react-query";

export type Role = {
  id: string;
  roleName: string;
  displayName: string;
  description: string | null;
  isSystemRole: boolean;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
};

export function useRoles() {
  return useQuery<{ roles: Role[] }, Error, Role[]>({
    queryKey: ['/api/roles'],
    select: (data) => data.roles,
  });
}

export function useActiveRoles() {
  return useQuery<{ roles: Role[] }, Error, Role[]>({
    queryKey: ['/api/roles'],
    select: (data) => data.roles.filter(role => role.isActive),
  });
}

export function useRoleNames() {
  return useQuery<{ roles: Role[] }, Error, string[]>({
    queryKey: ['/api/roles'],
    select: (data) => data.roles.filter(role => role.isActive).map(role => role.roleName),
  });
}
